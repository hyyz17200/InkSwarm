from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import os
import sys
import ctypes
import threading


from PySide6.QtCore import Qt, QUrl, QTimer, Signal
from PySide6.QtGui import QAction, QDesktopServices, QFont, QIcon, QPalette, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSizePolicy,
    QSpinBox,
    QSplitter,
    QStackedWidget,
    QStyle,
    QStyleOptionComboBox,
    QStylePainter,
    QTabBar,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from .config_store import ConfigStore
from .controller import PrintController
from .cache_service import CacheService
from .i18n import language_options, normalize_language, translate
from .local_logger import LocalLogWriter, RegularLogFilter
from .models import (
    CACHE_FORMAT_PNG_L1,
    CACHE_FORMAT_TIFF_DEFLATE,
    CACHE_FORMAT_TIFF_NONE,
    CACHE_IMAGE_FORMATS,
    SUPPORTED_INPUT_SUFFIXES,
    TaskItem,
    TaskStatusMessage,
    WorkerConfig,
    WorkerStatusMessage,
    normalize_cache_image_format,
    resolve_icc_path,
)
from .run_service import RunService
from .spooler_service import SpoolerMaintenance, SpoolerMaintenanceCancelled, run_elevated_spooler_maintenance
from .statistics_writer import CSV_HEADER, MonthlyStatisticsWriter
from .task_service import SkippedTaskInput, TaskService
from .worker_service import WorkerService, WorkerValidationError
from .debug_logger import debug_exception, debug_log, initialize_debug_logging, install_qt_message_handler


APP_NAME = "InkSwarm"
APP_VERSION = "0.3.7"
DEBUG_LOG_NAME = "debug.log"
DEFAULT_WINDOW_WIDTH = 1450
DEFAULT_WINDOW_HEIGHT = 940
# Edit these three values to tune the vertical pane heights: task, worker, log.
DEFAULT_VERTICAL_PANE_HEIGHTS = (320, 420, 320)
# Max log lines kept in the on-screen view (~7.5 KB/line); ~40 MB ceiling at 5000.
# Full history is still persisted to logs/ via LocalLogWriter, so nothing is lost.
LOG_VIEW_MAX_BLOCKS = 5000
STATISTICS_HIDDEN_COLUMNS = {"Run ID"}


def get_app_root() -> Path:
    try:
        return Path(__compiled__.containing_dir).resolve()  # type: ignore[name-defined]
    except NameError:
        return Path(os.path.dirname(sys.argv[0])).resolve()


class FileDropTable(QTableWidget):
    def __init__(self, on_files_dropped, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_files_dropped = on_files_dropped
        self.setAcceptDrops(True)
        self.setDragDropOverwriteMode(False)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event):
        files = []
        for url in event.mimeData().urls():
            if not url.isLocalFile():
                continue
            path = Path(url.toLocalFile())
            files.append(path)
        if files:
            self.on_files_dropped(files)
            event.acceptProposedAction()
        else:
            event.ignore()


class CenteredComboBox(QComboBox):
    def paintEvent(self, event) -> None:
        option = QStyleOptionComboBox()
        self.initStyleOption(option)
        painter = QStylePainter(self)
        painter.drawComplexControl(QStyle.ComplexControl.CC_ComboBox, option)

        text_rect = self.style().subControlRect(
            QStyle.ComplexControl.CC_ComboBox,
            option,
            QStyle.SubControl.SC_ComboBoxEditField,
            self,
        )
        color_group = QPalette.ColorGroup.Disabled if not self.isEnabled() else QPalette.ColorGroup.Active
        painter.setPen(option.palette.color(color_group, QPalette.ColorRole.ButtonText))
        text = option.fontMetrics.elidedText(self.currentText(), Qt.TextElideMode.ElideRight, text_rect.width())
        painter.drawText(
            text_rect,
            Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter,
            text,
        )


class MainWindow(QMainWindow):
    spooler_maintenance_log = Signal(str)
    spooler_maintenance_finished = Signal(object)

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} {APP_VERSION}")
        self.resize(DEFAULT_WINDOW_WIDTH, DEFAULT_WINDOW_HEIGHT)

        self.root_dir = get_app_root()
        self._apply_app_icon()
        self.store = ConfigStore(self.root_dir)
        self.task_service = TaskService(self.store)
        self.worker_service = WorkerService(self.store)
        self.run_service = RunService()
        self.cache_service = CacheService(self.store.paths.cache_dir, self.store.paths.preview_dir)
        self.controller = PrintController(self.store.paths.cache_dir, self.store.paths.statistics_dir)
        self.controller.signals.log.connect(self.on_log)
        self.controller.signals.task_status.connect(self.on_task_status)
        self.controller.signals.worker_status.connect(self.on_worker_status)
        self.controller.signals.run_state.connect(self.on_run_state_changed)
        self.controller.signals.pause_state.connect(self.on_pause_state_changed)
        self.controller.signals.spool_progress.connect(self.on_spool_progress)
        self.spooler_maintenance_log.connect(self.on_log_text)
        self.spooler_maintenance_finished.connect(self.on_spooler_maintenance_finished)
        self.log_writer = LocalLogWriter(self.store.paths.logs_dir)
        self.regular_log_filter = RegularLogFilter()
        self.statistics_report_reader = MonthlyStatisticsWriter(self.store.paths.statistics_dir)
        self._statistics_loaded_signatures: dict[str, tuple[str, bool, int, int]] = {}
        self.debug_log_path = (self.store.paths.logs_dir / DEBUG_LOG_NAME).resolve()
        debug_log(f"mainwindow init root_dir={self.root_dir}")
        self.app_settings = self.store.load_app_settings()
        self.app_settings["language"] = self.current_language()
        debug_log(f"settings loaded {self.app_settings}")
        self.set_console_visibility(False)
        self.current_worker_group = self.app_settings.get("active_worker_group", self.store.default_group_dir().name)
        self.resize(self._saved_window_width(), DEFAULT_WINDOW_HEIGHT)

        self.tasks: list[TaskItem] = []
        self.task_row_by_id: dict[str, int] = {}
        self.workers: list[WorkerConfig] = []
        self.worker_row_by_name: dict[str, int] = {}
        self.current_preview_pixmap: QPixmap | None = None
        self._spool_total = 0
        self._spooler_maintenance_active = False
        self.worker_config_error: str | None = None

        self._saved_ui_scale = int(self.app_settings.get("ui_scale", 100))
        self._ui_scale_applied_once = False

        self._build_ui()
        self.apply_ui_scale(100)
        if self.app_settings.get("auto_clear_cache_on_start", False):
            self.clear_cache_dir(log_message=False)
        self.refresh_worker_group_combo()
        self.reload_workers()
        if self.app_settings.get("save_tasks_on_exit", False):
            self.restore_task_session()
        if self.app_settings.get("auto_clear_cache_on_start", False):
            self.on_log_text(self.t("log.auto_cache_cleared"))
        QTimer.singleShot(0, self.apply_saved_startup_ui_state)

    def current_language(self) -> str:
        return normalize_language(self.app_settings.get("language", "en"))

    def t(self, key: str, **kwargs: object) -> str:
        return translate(self.current_language(), key, **kwargs)

    def _add_section_header(self, layout: QVBoxLayout, title: str) -> QLabel:
        label = QLabel(title)
        layout.addWidget(label)
        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setFrameShadow(QFrame.Shadow.Sunken)
        layout.addWidget(line)
        return label

    def _build_ui(self) -> None:
        self._build_menu_bar()

        central = QWidget()
        layout = QVBoxLayout(central)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self.main_splitter = QSplitter(Qt.Orientation.Vertical)

        top = QFrame()
        top.setObjectName("sectionPanel")
        top_layout = QVBoxLayout(top)
        top_layout.setContentsMargins(8, 8, 8, 8)
        top_layout.setSpacing(6)
        self.task_section_label = self._add_section_header(top_layout, self.t("title.task_section"))

        self.top_splitter = QSplitter(Qt.Orientation.Horizontal)

        self.task_table = FileDropTable(self.add_files)
        self.task_table.setColumnCount(5)
        self.task_table.setHorizontalHeaderLabels(self._task_table_headers())
        self.task_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.task_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.task_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.task_table.setAlternatingRowColors(True)
        self.task_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.task_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.task_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)
        self.task_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Fixed)
        self.task_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)
        self.task_table.setColumnWidth(0, 56)
        self.task_table.setColumnWidth(2, 88)
        self.task_table.setColumnWidth(3, 230)
        self.task_table.setColumnWidth(4, 250)
        self.task_table.itemSelectionChanged.connect(self.update_task_preview)
        self.top_splitter.addWidget(self.task_table)

        preview_panel = QWidget()
        preview_panel.setMinimumHeight(0)
        preview_panel.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Ignored)
        preview_layout = QVBoxLayout(preview_panel)
        preview_layout.setContentsMargins(0, 0, 0, 0)
        preview_layout.setSpacing(0)
        self.preview_label = QLabel()
        self.preview_label.setMinimumWidth(240)
        self.preview_label.setMinimumHeight(0)
        self.preview_label.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Ignored)
        self.preview_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.preview_label.setScaledContents(False)
        self.preview_label.setStyleSheet("border: 1px solid palette(mid); background: palette(base);")
        preview_layout.addWidget(self.preview_label, 1)
        self.top_splitter.addWidget(preview_panel)
        self.top_splitter.setSizes([1120, 260])
        self.top_splitter.setStretchFactor(0, 4)
        self.top_splitter.setStretchFactor(1, 1)
        top_layout.addWidget(self.top_splitter, 1)

        task_buttons = QHBoxLayout()
        self.btn_add_tasks = QPushButton(self.t("actions.add_files"))
        self.btn_add_tasks.clicked.connect(self.pick_files)
        self.btn_remove_tasks = QPushButton(self.t("actions.remove_selected"))
        self.btn_remove_tasks.clicked.connect(self.remove_selected_tasks)
        self.btn_clear_tasks = QPushButton(self.t("actions.clear_tasks"))
        self.btn_clear_tasks.clicked.connect(self.clear_tasks)
        self.btn_set_task_copies = QPushButton(self.t("actions.batch_copies"))
        self.btn_set_task_copies.clicked.connect(self.set_selected_task_copies)
        self.task_copies_value_box = QSpinBox()
        self.task_copies_value_box.setRange(1, 9999)
        self.task_copies_value_box.setValue(self._task_default_copies())
        self.task_copies_value_box.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.task_copies_value_box.setFixedWidth(88)
        self.task_copies_value_box.valueChanged.connect(self.on_task_default_copies_changed)
        for btn in [self.btn_add_tasks, self.btn_remove_tasks, self.btn_clear_tasks, self.btn_set_task_copies]:
            task_buttons.addWidget(btn)
        task_buttons.addWidget(self.task_copies_value_box)
        task_buttons.addStretch(1)
        top_layout.addLayout(task_buttons)

        self.bottom_splitter = QSplitter(Qt.Orientation.Vertical)

        worker_panel = QFrame()
        worker_panel.setObjectName("sectionPanel")
        worker_layout = QVBoxLayout(worker_panel)
        worker_layout.setContentsMargins(8, 8, 8, 8)
        worker_layout.setSpacing(6)
        self.worker_section_label = self._add_section_header(worker_layout, self.t("title.worker_section"))

        worker_content = QHBoxLayout()
        worker_content.setSpacing(8)

        worker_left = QWidget()
        worker_left_layout = QVBoxLayout(worker_left)
        worker_left_layout.setContentsMargins(0, 0, 0, 0)
        worker_left_layout.setSpacing(6)

        self.worker_table = QTableWidget()
        self.worker_table.setColumnCount(6)
        self.worker_table.setHorizontalHeaderLabels(self._worker_table_headers())
        self.worker_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.worker_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.worker_table.setAlternatingRowColors(True)
        header = self.worker_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        self.worker_table.setColumnWidth(0, 56)
        self.worker_table.setColumnWidth(1, 150)
        self.worker_table.setColumnWidth(2, 150)
        self.worker_table.setColumnWidth(3, 225)
        self.worker_table.setColumnWidth(4, 82)
        self.worker_table.setColumnWidth(5, 290)
        worker_left_layout.addWidget(self.worker_table, 1)

        worker_buttons = QHBoxLayout()
        self.worker_group_label = QLabel(self.t("worker_group"))
        worker_buttons.addWidget(self.worker_group_label)
        self.worker_group_combo = QComboBox()
        self.worker_group_combo.setMinimumWidth(150)
        self.worker_group_combo.currentIndexChanged.connect(self.on_worker_group_changed)
        worker_buttons.addWidget(self.worker_group_combo)

        self.btn_reload_workers = QPushButton(self.t("actions.reload_workers"))
        self.btn_reload_workers.clicked.connect(self.reload_workers)
        self.btn_save_workers = QPushButton(self.t("actions.save_worker_settings"))
        self.btn_save_workers.clicked.connect(self.save_worker_settings)
        self.btn_open_pref = QPushButton(self.t("actions.open_driver_preferences"))
        self.btn_open_pref.clicked.connect(self.open_selected_worker_preferences)
        self.btn_open_props = QPushButton(self.t("actions.open_printer_properties"))
        self.btn_open_props.clicked.connect(self.open_selected_worker_properties)
        self.btn_capture = QPushButton(self.t("actions.save_driver_settings"))
        self.btn_capture.clicked.connect(self.capture_selected_worker_snapshot)
        for btn in [self.btn_reload_workers, self.btn_save_workers, self.btn_open_pref, self.btn_open_props, self.btn_capture]:
            worker_buttons.addWidget(btn)
        worker_buttons.addStretch(1)
        worker_left_layout.addLayout(worker_buttons)

        controls_panel = QFrame()
        controls_panel.setObjectName("workerControlPanel")
        controls_panel.setMinimumWidth(180)
        controls_panel.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        controls_layout = QVBoxLayout(controls_panel)
        controls_layout.setContentsMargins(10, 10, 10, 10)
        controls_layout.setSpacing(10)

        self.start_button = QPushButton(self.t("actions.start"))
        self.start_button.clicked.connect(self.start_or_toggle_pause)
        self.start_button.setObjectName("primaryActionButton")
        self.start_button.setMinimumHeight(96)
        self.start_button.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        self.stop_button = QPushButton(self.t("actions.stop"))
        self.stop_button.clicked.connect(self.stop_run)
        self.stop_button.setObjectName("dangerActionButton")
        self.stop_button.setEnabled(False)
        self.stop_button.setMinimumHeight(96)
        self.stop_button.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        controls_layout.addWidget(self.start_button)
        controls_layout.addWidget(self.stop_button)

        self.spool_progress_bar = QProgressBar()
        self.spool_progress_bar.setRange(0, 1)
        self.spool_progress_bar.setValue(0)
        self.spool_progress_bar.setTextVisible(False)
        self.spool_progress_label = QLabel(self.t("spool.progress", sent=0, total=0))
        self.spool_progress_label.setObjectName("spoolProgressLabel")
        self.spool_progress_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.spool_progress_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        controls_layout.addWidget(self.spool_progress_label)
        controls_layout.addWidget(self.spool_progress_bar)
        controls_layout.addStretch(1)

        worker_content.addWidget(worker_left, 1)
        worker_content.addWidget(controls_panel, 0)
        worker_layout.addLayout(worker_content, 1)

        log_panel = QFrame()
        log_panel.setObjectName("sectionPanel")
        log_layout = QVBoxLayout(log_panel)
        log_layout.setContentsMargins(8, 8, 8, 8)
        log_layout.setSpacing(6)
        log_header = QHBoxLayout()
        self.log_card_tabs = QTabBar()
        self.log_card_tabs.setDrawBase(False)
        self.log_card_tabs.addTab(self.t("tab.log"))
        self.log_card_tabs.addTab(self.t("tab.today"))
        self.log_card_tabs.addTab(self.t("tab.current_month"))
        self.log_card_tabs.addTab(self.t("tab.previous_month"))
        self.log_card_tabs.currentChanged.connect(self.on_log_card_changed)
        log_header.addWidget(self.log_card_tabs)
        log_header.addStretch(1)
        log_layout.addLayout(log_header)

        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setFrameShadow(QFrame.Shadow.Sunken)
        log_layout.addWidget(line)

        self.log_stack = QStackedWidget()

        log_page = QWidget()
        log_page_layout = QVBoxLayout(log_page)
        log_page_layout.setContentsMargins(0, 0, 0, 0)
        log_page_layout.setSpacing(0)
        self.log_edit = QPlainTextEdit()
        self.log_edit.setReadOnly(True)
        # Cap the on-screen scrollback (~7.5 KB/line in QPlainTextEdit) so a long
        # production session can't grow unbounded; full history stays in logs/.
        self.log_edit.setMaximumBlockCount(LOG_VIEW_MAX_BLOCKS)
        self.log_edit.setUndoRedoEnabled(False)
        log_page_layout.addWidget(self.log_edit, 1)
        self.log_stack.addWidget(log_page)

        self.statistics_status_labels: dict[str, QLabel] = {}
        self.statistics_tables: dict[str, QTableWidget] = {}
        for statistics_key in ("today", "current_month", "previous_month"):
            statistics_page = QWidget()
            statistics_layout = QVBoxLayout(statistics_page)
            statistics_layout.setContentsMargins(0, 0, 0, 0)
            statistics_layout.setSpacing(6)
            statistics_status_label = QLabel("")
            statistics_table = QTableWidget()
            statistics_display_header = self._statistics_display_header(CSV_HEADER)
            statistics_table.setColumnCount(len(statistics_display_header))
            statistics_table.setHorizontalHeaderLabels(list(statistics_display_header))
            statistics_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
            statistics_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
            statistics_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
            statistics_table.setAlternatingRowColors(True)
            statistics_table.setWordWrap(False)
            statistics_header = statistics_table.horizontalHeader()
            for column in range(len(statistics_display_header)):
                statistics_header.setSectionResizeMode(column, QHeaderView.ResizeMode.ResizeToContents)
            if len(statistics_display_header) > 2:
                statistics_header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
            statistics_layout.addWidget(statistics_status_label)
            statistics_layout.addWidget(statistics_table, 1)
            self.statistics_status_labels[statistics_key] = statistics_status_label
            self.statistics_tables[statistics_key] = statistics_table
            self.log_stack.addWidget(statistics_page)
        log_layout.addWidget(self.log_stack, 1)
        self.log_card_tabs.setCurrentIndex(0)
        self.log_stack.setCurrentIndex(0)

        self.bottom_splitter.addWidget(worker_panel)
        self.bottom_splitter.addWidget(log_panel)

        self.main_splitter.addWidget(top)
        self.main_splitter.addWidget(self.bottom_splitter)
        self._apply_vertical_pane_layout(100)

        layout.addWidget(self.main_splitter)
        self.setCentralWidget(central)
        self.apply_language(refresh_statistics=False)

    def _build_menu_bar(self) -> None:
        menu = self.menuBar()
        self.settings_action = QAction(self.t("menu.settings"), self)
        self.settings_action.triggered.connect(self.open_settings_dialog)
        menu.addAction(self.settings_action)

        self.open_root_action = QAction(self.t("menu.program_dir"), self)
        self.open_root_action.triggered.connect(self.open_program_dir)
        menu.addAction(self.open_root_action)

        self.print_mgmt_action = QAction(self.t("menu.print_management"), self)
        self.print_mgmt_action.triggered.connect(self.open_print_management)
        menu.addAction(self.print_mgmt_action)

        self.restart_spooler_action = QAction(self.t("menu.restart_queue"), self)
        self.restart_spooler_action.triggered.connect(self.restart_print_queue)
        menu.addAction(self.restart_spooler_action)

        self.help_action = QAction(self.t("menu.about"), self)
        self.help_action.triggered.connect(self.open_help_dialog)
        menu.addAction(self.help_action)

    def open_settings_dialog(self) -> None:
        dialog = QDialog(self)
        dialog.setWindowTitle(self.t("settings.title"))
        dialog.setModal(True)
        dialog.resize(520, 500)

        layout = QVBoxLayout(dialog)
        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        language_combo = QComboBox()
        for code, label in language_options():
            language_combo.addItem(label, code)
        language_idx = language_combo.findData(self.current_language())
        if language_idx >= 0:
            language_combo.setCurrentIndex(language_idx)
        form.addRow(self.t("settings.language"), language_combo)

        autoclear_checkbox = QCheckBox(self.t("settings.auto_clear_cache"))
        autoclear_checkbox.setChecked(bool(self.app_settings.get("auto_clear_cache_on_start", False)))
        form.addRow(self.t("settings.cache"), autoclear_checkbox)

        save_tasks_checkbox = QCheckBox(self.t("settings.save_tasks"))
        save_tasks_checkbox.setChecked(bool(self.app_settings.get("save_tasks_on_exit", False)))
        form.addRow(self.t("settings.task"), save_tasks_checkbox)

        font_edit = QLineEdit(str(self.app_settings.get("font_family", "Segoe UI") or "Segoe UI"))
        font_edit.setClearButtonEnabled(True)
        form.addRow(self.t("settings.font"), font_edit)

        scale_combo = QComboBox()
        for value in [100, 125, 150, 175, 200]:
            scale_combo.addItem(f"{value}%", value)
        current_scale = int(self.app_settings.get("ui_scale", 100))
        idx = scale_combo.findData(current_scale)
        if idx >= 0:
            scale_combo.setCurrentIndex(idx)
        form.addRow(self.t("settings.ui_scale"), scale_combo)

        font_engine_combo = QComboBox()
        font_engine_combo.addItem(self.t("settings.font_engine.auto"), "auto")
        font_engine_combo.addItem(self.t("settings.font_engine.gdi"), "gdi")
        font_engine_combo.addItem(self.t("settings.font_engine.freetype"), "freetype")
        current_engine = str(self.app_settings.get("font_engine", "auto") or "auto").lower()
        engine_idx = font_engine_combo.findData(current_engine)
        if engine_idx >= 0:
            font_engine_combo.setCurrentIndex(engine_idx)
        form.addRow(self.t("settings.font_engine"), font_engine_combo)

        ignore_margins_checkbox = QCheckBox(self.t("settings.ignore_margins_text"))
        ignore_margins_checkbox.setChecked(bool(self.app_settings.get("ignore_margins", True)))
        form.addRow(self.t("settings.ignore_margins"), ignore_margins_checkbox)

        printer_defaults_checkbox = QCheckBox(self.t("status.enabled"))
        printer_defaults_checkbox.setChecked(bool(self.app_settings.get("printer_defaults_check_enabled", True)))
        form.addRow(self.t("settings.printer_defaults_check"), printer_defaults_checkbox)

        cmyk_fallback_row = QWidget()
        cmyk_fallback_layout = QHBoxLayout(cmyk_fallback_row)
        cmyk_fallback_layout.setContentsMargins(0, 0, 0, 0)
        cmyk_fallback_layout.setSpacing(6)
        cmyk_fallback_edit = QLineEdit(str(self.app_settings.get("cmyk_fallback_icc", "") or ""))
        cmyk_fallback_edit.setClearButtonEnabled(True)
        cmyk_fallback_edit.setPlaceholderText(self.t("settings.cmyk_fallback_icc_hint"))
        cmyk_fallback_browse = QPushButton(self.t("actions.browse"))

        def _browse_cmyk_fallback_icc() -> None:
            icc_dir = self.store.paths.icc_dir
            icc_dir.mkdir(parents=True, exist_ok=True)
            selected, _ = QFileDialog.getOpenFileName(
                dialog,
                self.t("file_dialog.select_cmyk_fallback_icc"),
                str(icc_dir),
                self.t("file_dialog.icc_files"),
            )
            if not selected:
                return
            selected_path = Path(selected)
            try:
                cmyk_fallback_edit.setText(str(selected_path.relative_to(icc_dir)))
            except ValueError:
                cmyk_fallback_edit.setText(str(selected_path))

        cmyk_fallback_browse.clicked.connect(_browse_cmyk_fallback_icc)
        cmyk_fallback_layout.addWidget(cmyk_fallback_edit, 1)
        cmyk_fallback_layout.addWidget(cmyk_fallback_browse, 0)
        form.addRow(self.t("settings.cmyk_fallback_icc"), cmyk_fallback_row)

        orient_enabled_checkbox = QCheckBox(self.t("status.enabled"))
        orient_enabled_checkbox.setChecked(bool(self.app_settings.get("auto_orient_enabled", False)))
        form.addRow(self.t("settings.auto_orient"), orient_enabled_checkbox)

        orientation_combo = QComboBox()
        orientation_combo.addItem(self.t("settings.orientation.portrait"), "portrait")
        orientation_combo.addItem(self.t("settings.orientation.landscape"), "landscape")
        orient_idx = orientation_combo.findData(str(self.app_settings.get("target_orientation", "portrait") or "portrait").lower())
        if orient_idx >= 0:
            orientation_combo.setCurrentIndex(orient_idx)
        orientation_combo.setEnabled(orient_enabled_checkbox.isChecked())
        orient_enabled_checkbox.toggled.connect(orientation_combo.setEnabled)
        form.addRow(self.t("settings.target_orientation"), orientation_combo)

        queue_limit_enabled_checkbox = QCheckBox(self.t("status.enabled"))
        queue_limit_enabled_checkbox.setChecked(bool(self.app_settings.get("worker_queue_limit_enabled", False)))
        form.addRow(self.t("settings.worker_queue_limit"), queue_limit_enabled_checkbox)

        queue_limit_spin = QSpinBox()
        queue_limit_spin.setRange(1, 999)
        queue_limit_spin.setValue(int(self.app_settings.get("worker_queue_limit", 3) or 3))
        queue_limit_spin.setEnabled(queue_limit_enabled_checkbox.isChecked())
        queue_limit_enabled_checkbox.toggled.connect(queue_limit_spin.setEnabled)
        form.addRow(self.t("settings.max_queue_value"), queue_limit_spin)

        tail_balance_enabled_checkbox = QCheckBox(self.t("status.enabled"))
        tail_balance_enabled_checkbox.setChecked(bool(self.app_settings.get("tail_balance_enabled", False)))
        form.addRow(self.t("settings.tail_balance"), tail_balance_enabled_checkbox)

        tail_balance_idle_spin = QSpinBox()
        tail_balance_idle_spin.setRange(1, 600)
        tail_balance_idle_spin.setSuffix(self.t("settings.seconds_suffix"))
        tail_balance_idle_spin.setValue(int(self.app_settings.get("tail_balance_idle_seconds", 15) or 15))
        tail_balance_idle_spin.setEnabled(tail_balance_enabled_checkbox.isChecked())
        tail_balance_enabled_checkbox.toggled.connect(tail_balance_idle_spin.setEnabled)
        form.addRow(self.t("settings.tail_idle_seconds"), tail_balance_idle_spin)

        rip_limit_enabled_checkbox = QCheckBox(self.t("status.enabled"))
        rip_limit_enabled_checkbox.setChecked(bool(self.app_settings.get("rip_limit_enabled", True)))
        form.addRow(self.t("settings.rip_limit"), rip_limit_enabled_checkbox)

        rip_limit_spin = QSpinBox()
        rip_limit_spin.setRange(72, 1200)
        rip_limit_spin.setSingleStep(25)
        rip_limit_spin.setValue(int(self.app_settings.get("rip_limit_ppi", 300) or 300))
        rip_limit_spin.setEnabled(rip_limit_enabled_checkbox.isChecked())
        rip_limit_enabled_checkbox.toggled.connect(rip_limit_spin.setEnabled)
        form.addRow(self.t("settings.max_ppi"), rip_limit_spin)

        cache_format_combo = QComboBox()
        cache_format_labels = {
            CACHE_FORMAT_TIFF_DEFLATE: self.t("settings.cache_format.tiff_deflate"),
            CACHE_FORMAT_TIFF_NONE: self.t("settings.cache_format.tiff_none"),
            CACHE_FORMAT_PNG_L1: self.t("settings.cache_format.png_l1"),
        }
        for cache_format in CACHE_IMAGE_FORMATS:
            cache_format_combo.addItem(cache_format_labels.get(cache_format, cache_format), cache_format)
        cache_format_idx = cache_format_combo.findData(
            normalize_cache_image_format(self.app_settings.get("cache_image_format"))
        )
        if cache_format_idx >= 0:
            cache_format_combo.setCurrentIndex(cache_format_idx)
        form.addRow(self.t("settings.cache_image_format"), cache_format_combo)

        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        ok_button = buttons.button(QDialogButtonBox.StandardButton.Ok)
        cancel_button = buttons.button(QDialogButtonBox.StandardButton.Cancel)
        if ok_button is not None:
            ok_button.setText(self.t("actions.ok"))
        if cancel_button is not None:
            cancel_button.setText(self.t("actions.cancel"))
        layout.addWidget(buttons)

        if dialog.exec() != QDialog.DialogCode.Accepted:
            return

        new_autoclear = bool(autoclear_checkbox.isChecked())
        new_save_tasks = bool(save_tasks_checkbox.isChecked())
        new_font = font_edit.text().strip() or "Segoe UI"
        new_scale = int(scale_combo.currentData())
        new_language = normalize_language(language_combo.currentData())

        self.app_settings["auto_clear_cache_on_start"] = new_autoclear
        self.app_settings["save_tasks_on_exit"] = new_save_tasks
        self.app_settings["font_family"] = new_font
        self.app_settings["ui_scale"] = new_scale
        self.app_settings["language"] = new_language
        self.app_settings["font_engine"] = str(font_engine_combo.currentData() or "auto")
        self.app_settings["ignore_margins"] = bool(ignore_margins_checkbox.isChecked())
        self.app_settings["printer_defaults_check_enabled"] = bool(printer_defaults_checkbox.isChecked())
        self.app_settings["cmyk_fallback_icc"] = cmyk_fallback_edit.text().strip()
        self.app_settings["auto_orient_enabled"] = bool(orient_enabled_checkbox.isChecked())
        self.app_settings["target_orientation"] = str(orientation_combo.currentData())
        self.app_settings["worker_queue_limit_enabled"] = bool(queue_limit_enabled_checkbox.isChecked())
        self.app_settings["worker_queue_limit"] = int(queue_limit_spin.value())
        self.app_settings["tail_balance_enabled"] = bool(tail_balance_enabled_checkbox.isChecked())
        self.app_settings["tail_balance_idle_seconds"] = int(tail_balance_idle_spin.value())
        self.app_settings["rip_limit_enabled"] = bool(rip_limit_enabled_checkbox.isChecked())
        self.app_settings["rip_limit_ppi"] = int(rip_limit_spin.value())
        self.app_settings["cache_image_format"] = normalize_cache_image_format(cache_format_combo.currentData())
        self.store.save_app_settings(self.app_settings)
        if not new_save_tasks:
            self.task_service.clear_session()
        self.apply_ui_scale(new_scale)
        self.apply_language(refresh_statistics=True)

    def open_help_dialog(self) -> None:
        github_url = "https://github.com/hyyz17200/InkSwarm"

        dialog = QDialog(self)
        dialog.setWindowTitle(self.t("about.title", app_name=APP_NAME))
        dialog.setModal(True)
        dialog.resize(460, 240)

        layout = QVBoxLayout(dialog)

        title = QLabel(f"{APP_NAME} {APP_VERSION}")
        title_font = title.font()
        title_font.setBold(True)
        title_font.setPointSizeF(title_font.pointSizeF() + 2)
        title.setFont(title_font)
        layout.addWidget(title)

        body = QLabel(self.t("about.body", github_url=github_url))
        body.setWordWrap(True)
        body.setTextFormat(Qt.TextFormat.RichText)
        body.setTextInteractionFlags(Qt.TextInteractionFlag.TextBrowserInteraction)
        body.setOpenExternalLinks(False)
        body.linkActivated.connect(lambda _link: QDesktopServices.openUrl(QUrl(github_url)))
        layout.addWidget(body)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        dialog.exec()

    def _apply_app_icon(self) -> None:
        for name in ("app.ico", "inkswarm.ico"):
            icon_path = self.root_dir / name
            if icon_path.exists():
                icon = QIcon(str(icon_path))
                self.setWindowIcon(icon)
                app = QApplication.instance()
                if isinstance(app, QApplication):
                    app.setWindowIcon(icon)
                break

    def _base_font(self) -> QFont:
        family = str(self.app_settings.get("font_family", "Segoe UI") or "Segoe UI")
        font = QFont(family)
        point_size = font.pointSizeF()
        if point_size <= 0:
            point_size = 9.0
        font.setPointSizeF(point_size)
        font.setStyleStrategy(QFont.StyleStrategy.PreferAntialias)
        try:
            font.setHintingPreference(QFont.HintingPreference.PreferFullHinting)
        except Exception:
            pass
        return font

    def _build_app_stylesheet(self, scale: int) -> str:
        base_radius = max(5, round(6 * scale / 100))
        selection_bg = "#2F6FEB"
        selection_fg = "#FFFFFF"
        return f"""
            QTableWidget {{
                gridline-color: palette(mid);
                alternate-background-color: rgba(127, 127, 127, 0.08);
            }}
            QTableWidget::item:selected,
            QTableView::item:selected {{
                background: {selection_bg};
                color: {selection_fg};
            }}
            QTableWidget::item:selected:!active,
            QTableView::item:selected:!active {{
                background: {selection_bg};
                color: {selection_fg};
            }}
            QHeaderView::section {{
                padding: 6px;
            }}
            #sectionPanel {{
                border: 1px solid palette(mid);
                border-radius: {base_radius}px;
                background: palette(base);
            }}
            #workerControlPanel {{
                border: 1px solid palette(mid);
                border-radius: {base_radius}px;
                background: palette(base);
            }}
            QMenuBar {{
                border-bottom: 1px solid palette(mid);
                padding: 2px;
            }}
            QMenuBar::item {{
                padding: 6px 10px;
                background: transparent;
                border-radius: {max(4, base_radius - 1)}px;
            }}
            QMenuBar::item:selected {{
                background: rgba(127, 127, 127, 0.14);
            }}
            QPushButton#primaryActionButton,
            QPushButton#dangerActionButton {{
                font-weight: 700;
                padding: {max(10, round(12 * scale / 100))}px;
            }}
        """

    def _task_table_headers(self) -> list[str]:
        return [
            self.t("task_table.enabled"),
            self.t("task_table.file"),
            self.t("task_table.copies"),
            self.t("task_table.print_size"),
            self.t("task_table.status"),
        ]

    def _worker_table_headers(self) -> list[str]:
        return [
            self.t("worker_table.enabled"),
            self.t("worker_table.worker"),
            self.t("worker_table.printer"),
            self.t("worker_table.preset"),
            self.t("worker_table.speed"),
            self.t("worker_table.status"),
        ]

    def apply_language(self, refresh_statistics: bool = True) -> None:
        self.setWindowTitle(f"{APP_NAME} {APP_VERSION}")
        self.settings_action.setText(self.t("menu.settings"))
        self.open_root_action.setText(self.t("menu.program_dir"))
        self.print_mgmt_action.setText(self.t("menu.print_management"))
        self.restart_spooler_action.setText(self.t("menu.restart_queue"))
        self.help_action.setText(self.t("menu.about"))

        self.task_section_label.setText(self.t("title.task_section"))
        self.worker_section_label.setText(self.t("title.worker_section"))
        self.worker_group_label.setText(self.t("worker_group"))

        self.task_table.setHorizontalHeaderLabels(self._task_table_headers())
        self.worker_table.setHorizontalHeaderLabels(self._worker_table_headers())
        self.btn_add_tasks.setText(self.t("actions.add_files"))
        self.btn_remove_tasks.setText(self.t("actions.remove_selected"))
        self.btn_clear_tasks.setText(self.t("actions.clear_tasks"))
        self.btn_set_task_copies.setText(self.t("actions.batch_copies"))
        self.btn_reload_workers.setText(self.t("actions.reload_workers"))
        self.btn_save_workers.setText(self.t("actions.save_worker_settings"))
        self.btn_open_pref.setText(self.t("actions.open_driver_preferences"))
        self.btn_open_props.setText(self.t("actions.open_printer_properties"))
        self.btn_capture.setText(self.t("actions.save_driver_settings"))
        self.stop_button.setText(self.t("actions.stop"))
        self._refresh_start_button_text()
        self._set_spool_progress_text(self.spool_progress_bar.value())

        self.log_card_tabs.setTabText(0, self.t("tab.log"))
        self.log_card_tabs.setTabText(1, self.t("tab.today"))
        self.log_card_tabs.setTabText(2, self.t("tab.current_month"))
        self.log_card_tabs.setTabText(3, self.t("tab.previous_month"))

        for table in self.statistics_tables.values():
            table.setHorizontalHeaderLabels(list(self._statistics_display_header(CSV_HEADER)))

        self.refresh_worker_group_combo()
        self.refresh_task_table()
        for row, worker in enumerate(self.workers):
            status_item = self.worker_table.item(row, 5)
            if status_item is not None:
                raw_status = status_item.data(Qt.ItemDataRole.UserRole)
                self._set_worker_status_item(row, str(raw_status or "Idle"))
            elif row < len(self.workers):
                self._set_worker_status_item(row, "Idle")
        self._statistics_loaded_signatures.clear()
        if refresh_statistics and self._statistics_card_context() is not None:
            self.refresh_statistics_table(force=True)

    def _refresh_start_button_text(self) -> None:
        if self.controller.is_running():
            key = "actions.resume" if self.controller.is_paused() else "actions.pause"
        else:
            key = "actions.start"
        self.start_button.setText(self.t(key))

    def _set_spool_progress_text(self, sent: int) -> None:
        self.spool_progress_label.setText(self.t("spool.progress", sent=int(sent), total=self._spool_total))

    @staticmethod
    def _scaled_vertical_pane_heights(scale: int) -> tuple[int, int, int]:
        task_height, worker_height, log_height = DEFAULT_VERTICAL_PANE_HEIGHTS
        normalized_scale = max(1, int(scale))
        return (
            max(1, round(task_height * normalized_scale / 100)),
            max(1, round(worker_height * normalized_scale / 100)),
            max(1, round(log_height * normalized_scale / 100)),
        )

    def _apply_vertical_pane_layout(self, scale: int) -> None:
        self._apply_vertical_pane_heights(self._scaled_vertical_pane_heights(scale))

    def _apply_vertical_pane_heights(self, pane_heights: tuple[int, int, int]) -> None:
        task_height, worker_height, log_height = pane_heights
        lower_height = worker_height + log_height
        self.main_splitter.setSizes([task_height, lower_height])
        self.bottom_splitter.setSizes([worker_height, log_height])
        self.bottom_splitter.setStretchFactor(0, worker_height)
        self.bottom_splitter.setStretchFactor(1, log_height)
        self.main_splitter.setStretchFactor(0, task_height)
        self.main_splitter.setStretchFactor(1, lower_height)

    def apply_ui_scale(self, scale: int) -> None:
        app = QApplication.instance()
        if isinstance(app, QApplication):
            base_font = self._base_font()
            scaled_font = QFont(base_font)
            scaled_font.setPointSizeF(max(7.5, base_font.pointSizeF() * scale / 100.0))
            app.setFont(scaled_font)
            app.setStyleSheet(self._build_app_stylesheet(scale))

        row_size = max(28, round(30 * scale / 100))
        self.task_table.verticalHeader().setDefaultSectionSize(row_size)
        self.worker_table.verticalHeader().setDefaultSectionSize(row_size)
        button_height = max(88, round(96 * scale / 100))
        self.start_button.setMinimumHeight(button_height)
        self.stop_button.setMinimumHeight(button_height)
        self.start_button.setMaximumHeight(button_height + max(12, round(20 * scale / 100)))
        self.stop_button.setMaximumHeight(button_height + max(12, round(20 * scale / 100)))
        self.spool_progress_bar.setMinimumHeight(max(24, round(28 * scale / 100)))
        self.worker_table.setColumnWidth(0, max(54, round(56 * scale / 100)))
        self.worker_table.setColumnWidth(1, max(130, round(150 * scale / 100)))
        self.worker_table.setColumnWidth(2, max(130, round(150 * scale / 100)))
        self.worker_table.setColumnWidth(3, max(180, round(225 * scale / 100)))
        self.worker_table.setColumnWidth(4, max(76, round(82 * scale / 100)))
        self.worker_table.setColumnWidth(5, max(110, round(290 * scale / 100)))
        self.task_copies_value_box.setFixedWidth(max(82, round(88 * scale / 100)))
        self.task_table.setColumnWidth(0, max(54, round(56 * scale / 100)))
        self.task_table.setColumnWidth(2, max(82, round(88 * scale / 100)))
        size_width = max(210, round(230 * scale / 100))
        status_width = max(140, round(250 * scale / 100))
        self.task_table.setColumnWidth(3, size_width)
        self.task_table.setColumnWidth(4, status_width)
        self.top_splitter.setSizes([max(860, round(1120 * scale / 100)), max(220, round(260 * scale / 100))])
        self._apply_vertical_pane_layout(scale)
        self.update_task_preview()

    def apply_saved_startup_ui_state(self) -> None:
        if self._ui_scale_applied_once:
            return
        self._ui_scale_applied_once = True
        self.apply_ui_scale(self._saved_ui_scale)
        self._restore_saved_window_layout()
        self.set_console_visibility(False)

    @staticmethod
    def _coerce_int(value: object) -> int | None:
        if not isinstance(value, (str, int, float)):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_window_width(value: object) -> int:
        width = MainWindow._coerce_int(value)
        if width is None:
            return DEFAULT_WINDOW_WIDTH
        return max(800, width)

    @staticmethod
    def _coerce_vertical_pane_heights(value: object) -> tuple[int, int, int] | None:
        if not isinstance(value, (list, tuple)) or len(value) != 3:
            return None
        heights: list[int] = []
        for item in value:
            height = MainWindow._coerce_int(item)
            if height is None:
                return None
            if height < 0:
                return None
            heights.append(height)
        if sum(heights) <= 0:
            return None
        return heights[0], heights[1], heights[2]

    def _saved_window_width(self) -> int:
        return self._coerce_window_width(self.app_settings.get("window_width", DEFAULT_WINDOW_WIDTH))

    def _saved_vertical_pane_heights(self) -> tuple[int, int, int] | None:
        return self._coerce_vertical_pane_heights(self.app_settings.get("vertical_pane_heights"))

    def _restore_saved_window_layout(self) -> None:
        self.resize(self._saved_window_width(), self.height())
        pane_heights = self._saved_vertical_pane_heights()
        if pane_heights is not None:
            self._apply_vertical_pane_heights(pane_heights)

    def _current_vertical_pane_heights(self) -> tuple[int, int, int] | None:
        main_sizes = self.main_splitter.sizes()
        bottom_sizes = self.bottom_splitter.sizes()
        if len(main_sizes) < 2 or len(bottom_sizes) < 2:
            return None
        pane_heights = (int(main_sizes[0]), int(bottom_sizes[0]), int(bottom_sizes[1]))
        return self._coerce_vertical_pane_heights(pane_heights)

    def _remember_window_layout(self) -> None:
        self.app_settings["window_width"] = int(self.width())
        pane_heights = self._current_vertical_pane_heights()
        if pane_heights is not None:
            self.app_settings["vertical_pane_heights"] = list(pane_heights)

    def _display_worker_group_name(self, group_name: str) -> str:
        if group_name == "Workers":
            return self.t("worker.default_group")
        if group_name == "workers":
            return self.t("worker.default_legacy_group")
        if group_name.startswith("Workers_"):
            return group_name[len("Workers_"):] or self.t("worker.default_group")
        return group_name

    def set_console_visibility(self, visible: bool) -> None:
        if sys.platform != "win32":
            return
        try:
            hwnd = ctypes.windll.kernel32.GetConsoleWindow()
            if not hwnd:
                return
            ctypes.windll.user32.ShowWindow(hwnd, 5 if visible else 0)
        except Exception:
            pass

    def refresh_worker_group_combo(self) -> None:
        groups = self.worker_service.list_groups()
        self.worker_group_combo.blockSignals(True)
        self.worker_group_combo.clear()
        for name in groups:
            self.worker_group_combo.addItem(self._display_worker_group_name(name), name)
        index = self.worker_group_combo.findData(self.current_worker_group)
        if index < 0 and groups:
            self.current_worker_group = groups[0]
            index = 0
        if index >= 0:
            self.worker_group_combo.setCurrentIndex(index)
        self.worker_group_combo.blockSignals(False)

    def on_worker_group_changed(self) -> None:
        group_name = self.worker_group_combo.currentData()
        if not group_name:
            return
        self.current_worker_group = str(group_name)
        self.app_settings["active_worker_group"] = self.current_worker_group
        self.store.save_app_settings(self.app_settings)
        self.reload_workers()

    def restore_task_session(self) -> None:
        result = self.task_service.restore_saved_tasks(
            self.tasks,
            language=self.current_language(),
            cmyk_fallback_icc=self._cmyk_fallback_icc_path(),
        )
        if result.requested_count <= 0:
            return
        self.refresh_task_table()
        for skipped in result.add_result.skipped:
            self.on_log_text(self.t("log.skip_file", file_name=skipped.file_path.name, reason=self._task_skip_reason_text(skipped)))
        if result.add_result.added_count:
            self.on_log_text(self.t("log.tasks_added", count=result.add_result.added_count))
        self.on_log_text(self.t("log.restored_tasks", count=result.requested_count))

    def _set_task_editing_enabled(self, enabled: bool) -> None:
        for widget in (
            self.btn_add_tasks,
            self.btn_remove_tasks,
            self.btn_clear_tasks,
            self.btn_set_task_copies,
            self.task_copies_value_box,
        ):
            widget.setEnabled(enabled)
        self.task_table.setAcceptDrops(enabled)

    def pick_files(self) -> None:
        if self.controller.is_running():
            return
        files, _ = QFileDialog.getOpenFileNames(
            self,
            self.t("file_dialog.print_files"),
            str(self.root_dir),
            self.t("file_dialog.supported_files"),
        )
        self.add_files([Path(f) for f in files])

    def add_files(self, files: list[Path]) -> None:
        if self.controller.is_running():
            return
        result = self.task_service.add_files(
            self.tasks,
            files,
            default_copies=self.task_copies_value_box.value(),
            language=self.current_language(),
            cmyk_fallback_icc=self._cmyk_fallback_icc_path(),
        )
        self.refresh_task_table()
        for skipped in result.skipped:
            self.on_log_text(self.t("log.skip_file", file_name=skipped.file_path.name, reason=self._task_skip_reason_text(skipped)))
        if result.added_count:
            self.on_log_text(self.t("log.tasks_added", count=result.added_count))

    def _task_skip_reason_text(self, skipped: SkippedTaskInput) -> str:
        if skipped.reason_key:
            return self.t(skipped.reason_key)
        return skipped.reason

    def _task_status_text(self, status: str) -> str:
        status_text = (status or "").strip()
        translations = {
            "Pending": self.t("status.pending"),
            "Waiting": self.t("status.waiting"),
            "Scheduling": self.t("status.scheduling"),
            "Queued": self.t("status.queued"),
            "Done": self.t("status.done"),
            "Error": self.t("status.error"),
            "Disabled": self.t("status.disabled"),
        }
        if status_text.startswith("Printing "):
            return self.t("status.printing_detail", detail=status_text[len("Printing "):])
        return translations.get(status_text, status_text)

    def _worker_status_text(self, status: str) -> str:
        status_text = (status or "").strip()
        translations = {
            "Idle": self.t("status.idle"),
            "Stopped": self.t("status.stopped"),
            "Stopping": self.t("status.stopping"),
            "Error": self.t("status.error"),
            "Paused": self.t("status.paused"),
        }
        if status_text.startswith("Paused "):
            return self.t("status.paused_detail", detail=status_text[len("Paused "):])
        if status_text.startswith("Preparing "):
            return self.t("status.preparing_detail", detail=status_text[len("Preparing "):])
        if status_text.startswith("Printing "):
            return self.t("status.printing_detail", detail=status_text[len("Printing "):])
        if status_text.startswith("Waiting printer "):
            return self.t("status.waiting_printer_detail", detail=status_text[len("Waiting printer "):])
        if status_text.startswith("Queue "):
            detail = status_text[len("Queue "):]
            if detail.endswith(", pausing send"):
                detail = detail[: -len(", pausing send")]
            return self.t("status.queue_detail", detail=detail.strip())
        return translations.get(status_text, status_text)

    def _set_worker_status_item(self, row: int, status: str) -> None:
        item = self.worker_table.item(row, 5)
        if item is None:
            item = QTableWidgetItem()
            item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.worker_table.setItem(row, 5, item)
        display_status = self._worker_status_text(status)
        item.setData(Qt.ItemDataRole.UserRole, status)
        item.setText(display_status)
        item.setToolTip(display_status)

    def refresh_task_table(self) -> None:
        task_editing_enabled = not self.controller.is_running()
        self.task_table.setRowCount(len(self.tasks))
        self.task_row_by_id.clear()
        for row, task in enumerate(self.tasks):
            self.task_row_by_id[task.task_id] = row

            enabled_box = QCheckBox()
            enabled_box.setChecked(task.enabled)
            enabled_box.setEnabled(task_editing_enabled)
            enabled_box.toggled.connect(lambda checked, task_id=task.task_id: self.on_task_enabled_changed(task_id, checked))
            self.task_table.setCellWidget(row, 0, self._centered_widget(enabled_box))

            file_item = QTableWidgetItem(task.file_name())
            file_item.setData(Qt.ItemDataRole.UserRole, task.task_id)
            file_item.setFlags(file_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.task_table.setItem(row, 1, file_item)

            copies_box = QSpinBox()
            copies_box.setRange(1, 9999)
            copies_box.setAlignment(Qt.AlignmentFlag.AlignCenter)
            copies_box.setValue(task.copies)
            copies_box.setEnabled(task_editing_enabled)
            copies_box.valueChanged.connect(lambda value, task_id=task.task_id: self.on_task_copies_changed(task_id, value))
            self.task_table.setCellWidget(row, 2, copies_box)

            size_item = QTableWidgetItem(task.display_size_mm)
            size_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            size_item.setFlags(size_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.task_table.setItem(row, 3, size_item)

            status_item = QTableWidgetItem(self._task_status_text(task.status))
            status_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            status_item.setFlags(status_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.task_table.setItem(row, 4, status_item)

        self.update_task_preview()

    def on_task_enabled_changed(self, task_id: str, enabled: bool) -> None:
        if self.controller.is_running():
            return
        task = self.task_service.set_task_enabled(self.tasks, task_id, enabled)
        if task is not None:
            self.refresh_task_row(task)

    def _task_default_copies(self) -> int:
        try:
            value = int(self.app_settings.get("task_default_copies", 1) or 1)
        except (TypeError, ValueError):
            value = 1
        return max(1, min(9999, value))

    def on_task_default_copies_changed(self, value: int) -> None:
        self.app_settings["task_default_copies"] = int(value)

    def on_task_copies_changed(self, task_id: str, value: int) -> None:
        if self.controller.is_running():
            return
        self.task_service.set_task_copies(self.tasks, task_id, value)

    def remove_selected_tasks(self) -> None:
        if self.controller.is_running():
            return
        rows = {index.row() for index in self.task_table.selectedIndexes()}
        self.task_service.remove_rows(self.tasks, rows)
        self.refresh_task_table()

    def clear_tasks(self) -> None:
        if self.controller.is_running():
            QMessageBox.warning(self, self.t("dialog.run_active"), self.t("message.cannot_modify_running"))
            return
        self.task_service.clear(self.tasks)
        self.refresh_task_table()

    def set_selected_task_copies(self) -> None:
        if self.controller.is_running():
            return
        rows = sorted({index.row() for index in self.task_table.selectedIndexes()})
        if not rows:
            return
        value = self.task_copies_value_box.value()
        self.task_service.set_rows_copies(self.tasks, rows, value)
        self.refresh_task_table()

    def _centered_widget(self, child: QWidget) -> QWidget:
        wrapper = QWidget()
        layout = QHBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(child)
        return wrapper

    def reload_workers(self) -> None:
        self.workers = self.worker_service.load_workers(self.current_worker_group)
        self.worker_config_error = None
        self.worker_table.setRowCount(len(self.workers))
        self.worker_row_by_name.clear()
        for row, worker in enumerate(self.workers):
            self.worker_row_by_name[worker.name] = row

            enabled_box = QCheckBox()
            enabled_box.setChecked(worker.enabled)
            self.worker_table.setCellWidget(row, 0, self._centered_widget(enabled_box))

            name_item = QTableWidgetItem(worker.name)
            name_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            name_item.setFlags(name_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.worker_table.setItem(row, 1, name_item)

            printer_item = QTableWidgetItem(worker.printer_name)
            printer_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.worker_table.setItem(row, 2, printer_item)

            preset_combo = CenteredComboBox()
            preset_names = sorted(worker.presets.keys())
            for preset_name in preset_names:
                preset_combo.addItem(preset_name)
                preset_combo.setItemData(
                    preset_combo.count() - 1,
                    Qt.AlignmentFlag.AlignCenter,
                    Qt.ItemDataRole.TextAlignmentRole,
                )
            active_index = preset_combo.findText(worker.active_preset)
            if active_index >= 0:
                preset_combo.setCurrentIndex(active_index)
            self.worker_table.setCellWidget(row, 3, preset_combo)

            weight_box = QSpinBox()
            weight_box.setRange(1, 100)
            weight_box.setAlignment(Qt.AlignmentFlag.AlignCenter)
            weight_box.setValue(worker.weight)
            self.worker_table.setCellWidget(row, 4, weight_box)

            self._set_worker_status_item(row, "Idle")

        self.on_log_text(self.t("log.worker_group_loaded", group_name=self.current_worker_group, count=len(self.workers)))
        try:
            self.worker_service.validate_workers(self.workers, language=self.current_language())
        except WorkerValidationError as exc:
            self.worker_config_error = str(exc)
            self.on_log_text(self.t("log.worker_config_invalid", error=exc))

    def save_worker_settings(self) -> None:
        for row, worker in enumerate(self.workers):
            enabled_wrapper = self.worker_table.cellWidget(row, 0)
            enabled_box = enabled_wrapper.findChild(QCheckBox) if enabled_wrapper else None
            preset_combo = self.worker_table.cellWidget(row, 3)
            weight_box = self.worker_table.cellWidget(row, 4)
            worker.enabled = bool(enabled_box.isChecked()) if enabled_box is not None else worker.enabled
            printer_item = self.worker_table.item(row, 2)
            worker.printer_name = printer_item.text().strip() if printer_item is not None else worker.printer_name
            worker.active_preset = preset_combo.currentText() if isinstance(preset_combo, QComboBox) else worker.active_preset
            worker.weight = int(weight_box.value()) if isinstance(weight_box, QSpinBox) else worker.weight
        self.worker_service.save_workers(self.workers)
        self.on_log_text(self.t("log.worker_settings_saved"))

    def _selected_worker(self) -> WorkerConfig | None:
        row = self.worker_table.currentRow()
        if row < 0 or row >= len(self.workers):
            return None
        self.save_worker_settings()
        return self.workers[row]

    def _printer_defaults_check_enabled(self) -> bool:
        return bool(self.app_settings.get("printer_defaults_check_enabled", True))

    def _cmyk_fallback_icc_path(self) -> Path | None:
        return resolve_icc_path(self.store.paths.icc_dir, str(self.app_settings.get("cmyk_fallback_icc", "") or ""))

    def _restore_worker_preset_if_any(self, worker: WorkerConfig) -> None:
        message = self.worker_service.restore_preset_if_any(
            worker,
            initialize_defaults=self._printer_defaults_check_enabled(),
            language=self.current_language(),
        )
        if message:
            self.on_log_text(message)

    def open_selected_worker_preferences(self) -> None:
        worker = self._selected_worker()
        if worker is None:
            QMessageBox.information(self, self.t("dialog.info"), self.t("message.no_selected_worker"))
            return
        if not worker.printer_name:
            QMessageBox.warning(self, self.t("dialog.info"), self.t("message.no_printer_name"))
            return
        try:
            self._restore_worker_preset_if_any(worker)
            self.worker_service.open_preferences(worker, language=self.current_language())
        except Exception as exc:
            QMessageBox.critical(self, self.t("dialog.open_failed"), str(exc))

    def open_selected_worker_properties(self) -> None:
        worker = self._selected_worker()
        if worker is None:
            QMessageBox.information(self, self.t("dialog.info"), self.t("message.no_selected_worker"))
            return
        if not worker.printer_name:
            QMessageBox.warning(self, self.t("dialog.info"), self.t("message.no_printer_name"))
            return
        try:
            self.worker_service.open_properties(worker, language=self.current_language())
        except Exception as exc:
            QMessageBox.critical(self, self.t("dialog.open_failed"), str(exc))

    def capture_selected_worker_snapshot(self) -> None:
        worker = self._selected_worker()
        if worker is None:
            QMessageBox.information(self, self.t("dialog.info"), self.t("message.no_selected_worker"))
            return
        if not worker.printer_name:
            QMessageBox.warning(self, self.t("dialog.info"), self.t("message.no_printer_name"))
            return
        try:
            preset_name = worker.get_active_preset().name
            snapshot_path = self.worker_service.capture_snapshot(
                worker,
                initialize_defaults=self._printer_defaults_check_enabled(),
                language=self.current_language(),
            )
            self.on_log_text(
                self.t(
                    "worker.exported_snapshot",
                    worker_name=worker.name,
                    preset_name=preset_name,
                    snapshot_name=snapshot_path.name,
                )
            )
        except Exception as exc:
            QMessageBox.critical(self, self.t("dialog.error"), str(exc))

    def start_or_toggle_pause(self) -> None:
        if self._spooler_maintenance_active:
            QMessageBox.information(self, self.t("dialog.processing"), self.t("spool.restart_running"))
            return
        if self.controller.is_running():
            self.controller.toggle_pause()
            return
        self.start_run()

    def start_run(self) -> None:
        if not self.tasks:
            QMessageBox.information(self, self.t("dialog.info"), self.t("message.add_tasks_first"))
            return
        if not any(task.enabled for task in self.tasks):
            QMessageBox.information(self, self.t("dialog.info"), self.t("message.select_enabled_task"))
            return
        self.save_worker_settings()
        try:
            self.worker_service.validate_workers(self.workers, language=self.current_language())
        except WorkerValidationError as exc:
            self.worker_config_error = str(exc)
            self.on_log_text(self.t("log.worker_config_invalid", error=exc))
            QMessageBox.critical(
                self,
                self.t("message.worker_config_invalid"),
                self.t("message.worker_config_invalid_detail", error=exc),
            )
            return
        try:
            self.controller.validate_environment(language=self.current_language())
        except Exception as exc:
            debug_exception("MainWindow.start_run.environment", exc)
            QMessageBox.critical(self, self.t("message.environment_check_failed"), str(exc))
            return
        self.task_service.reset_for_run(self.tasks)
        self.refresh_task_table()
        prepared = self.run_service.prepare_start(
            self.tasks,
            self.workers,
            self.app_settings,
            icc_dir=self.store.paths.icc_dir,
        )
        self._spool_total = prepared.spool_total
        self.spool_progress_bar.setRange(0, max(1, self._spool_total))
        self.spool_progress_bar.setValue(0)
        self._set_spool_progress_text(0)
        debug_log(f"start_run with options={prepared.run_options} tasks={[(t.file_name(), t.copies) for t in prepared.tasks]}")
        self.regular_log_filter.reset()
        try:
            self.controller.start(prepared.tasks, prepared.workers, prepared.run_options)
        except Exception as exc:
            QMessageBox.critical(self, self.t("dialog.start_failed"), str(exc))

    def stop_run(self) -> None:
        if not self.controller.is_running():
            return
        self.controller.stop()

    def restart_print_queue(self) -> None:
        if self._spooler_maintenance_active:
            QMessageBox.information(self, self.t("dialog.processing"), self.t("spool.maintenance_active"))
            return

        dialog = QMessageBox(self)
        dialog.setIcon(QMessageBox.Icon.Warning)
        dialog.setWindowTitle(self.t("spool.dialog_title"))
        dialog.setText(self.t("spool.dialog_text"))
        dialog.setInformativeText(self.t("spool.dialog_warning"))
        restart_button = dialog.addButton(self.t("spool.confirm_restart"), QMessageBox.ButtonRole.AcceptRole)
        cancel_button = dialog.addButton(self.t("actions.cancel"), QMessageBox.ButtonRole.RejectRole)
        dialog.setDefaultButton(cancel_button)
        dialog.setEscapeButton(cancel_button)
        dialog.exec()

        clicked = dialog.clickedButton()
        if clicked == restart_button:
            self.start_spooler_maintenance()

    def start_spooler_maintenance(self) -> None:
        self._spooler_maintenance_active = True
        self.restart_spooler_action.setEnabled(False)
        was_running = self.controller.is_running()
        if was_running:
            self.on_log_text(self.t("spool.maintenance_pause_log"))
            self.controller.pause()
        else:
            self.on_log_text(self.t("spool.maintenance_start_log"))

        thread = threading.Thread(
            target=self._run_spooler_maintenance,
            args=(was_running, self.current_language()),
            daemon=True,
            name="SpoolerMaintenance",
        )
        thread.start()

    def _run_spooler_maintenance(self, resume_after_success: bool, language: str) -> None:
        language = normalize_language(language)

        def t(key: str, **kwargs: object) -> str:
            return translate(language, key, **kwargs)

        try:
            if resume_after_success:
                self.spooler_maintenance_log.emit(t("spool.pause_wait_log"))
                if not self.controller.wait_until_paused_idle(timeout_seconds=15.0):
                    raise RuntimeError(t("spool.pause_wait_failed"))
                self.spooler_maintenance_log.emit(t("spool.paused_log"))

            if SpoolerMaintenance.is_process_elevated():
                maintenance = SpoolerMaintenance(timeout_seconds=120.0, language=language)
                result = maintenance.restart(
                    log=lambda message: self.spooler_maintenance_log.emit(message),
                )
            else:
                self.spooler_maintenance_log.emit(t("spool.request_admin_log"))
                result = run_elevated_spooler_maintenance(
                    timeout_seconds=120.0,
                    log=lambda message: self.spooler_maintenance_log.emit(message),
                    language=language,
                )
            self.spooler_maintenance_finished.emit(
                {
                    "ok": True,
                    "resume_after_success": resume_after_success,
                }
            )
        except SpoolerMaintenanceCancelled as exc:
            self.spooler_maintenance_finished.emit(
                {
                    "ok": False,
                    "cancelled": True,
                    "was_running": resume_after_success,
                    "error": str(exc),
                }
            )
        except Exception as exc:
            debug_exception("MainWindow._run_spooler_maintenance", exc)
            self.spooler_maintenance_finished.emit(
                {
                    "ok": False,
                    "was_running": resume_after_success,
                    "error": str(exc),
                }
            )

    def on_spooler_maintenance_finished(self, result: object) -> None:
        self._spooler_maintenance_active = False
        self.restart_spooler_action.setEnabled(True)

        data = result if isinstance(result, dict) else {}
        if data.get("ok"):
            if data.get("resume_after_success") and self.controller.is_running():
                self.controller.resume()
            QMessageBox.information(self, self.t("spool.restart_complete_title"), self.t("spool.restart_complete"))
            return

        error = str(data.get("error") or self.t("spool.unknown_error"))
        if data.get("cancelled"):
            if data.get("was_running") and self.controller.is_running():
                self.controller.resume()
            QMessageBox.information(self, self.t("actions.cancel"), error)
            return
        pause_text = ""
        if data.get("was_running") and self.controller.is_running():
            pause_text = self.t("spool.stays_paused")
        QMessageBox.warning(
            self,
            self.t("spool.maintenance_failed"),
            f"{error}{pause_text}",
        )

    def on_log(self, message) -> None:
        self.on_log_text(message.format(), once_key=getattr(message, "once_key", None))

    def on_log_card_changed(self, index: int) -> None:
        self.log_stack.setCurrentIndex(index)
        showing_statistics = self._statistics_card_context(index) is not None
        if showing_statistics:
            self.refresh_statistics_table()

    def _statistics_card_context(self, index: int | None = None) -> tuple[str, str, str | None] | None:
        current_index = self.log_card_tabs.currentIndex() if index is None else index
        today = date.today()
        if current_index == 1:
            return ("today", today.strftime("%Y-%m-%d"), None)
        if current_index == 2:
            month = today.strftime("%Y-%m")
            return ("current_month", f"{month}.csv", month)
        if current_index == 3:
            previous_month_day = today.replace(day=1) - timedelta(days=1)
            previous_month = previous_month_day.strftime("%Y-%m")
            return ("previous_month", f"{previous_month}.csv", previous_month)
        return None

    def refresh_statistics_table(self, force: bool = False) -> None:
        context = self._statistics_card_context()
        if context is None:
            return
        statistics_key, display_label, month = context
        status_label = self.statistics_status_labels[statistics_key]
        table = self.statistics_tables[statistics_key]
        signature = self._statistics_report_signature(statistics_key, display_label, month)
        if not force and self._statistics_loaded_signatures.get(statistics_key) == signature:
            return
        try:
            if statistics_key == "today":
                report = self.statistics_report_reader.read_daily_report(display_label)
            else:
                report = self.statistics_report_reader.read_monthly_report(month)
        except Exception as exc:
            debug_exception("MainWindow.refresh_statistics_table", exc)
            status_label.setText(self.t("statistics.csv_read_failed", error=exc))
            table.setRowCount(0)
            return

        self._populate_statistics_table(table, report.header, report.rows)
        self._statistics_loaded_signatures[statistics_key] = self._statistics_report_signature(
            statistics_key,
            display_label,
            month,
        )
        status_label.setText(
            self._statistics_status_text(
                statistics_key,
                display_label,
                report.exists,
                len(report.rows),
                report.total_success_copies,
            )
        )

    def _statistics_report_signature(self, statistics_key: str, display_label: str, month: str | None) -> tuple[str, bool, int, int]:
        if statistics_key == "today":
            csv_path = self.store.paths.statistics_dir / f"{display_label[:7]}.csv"
        else:
            csv_path = self.store.paths.statistics_dir / f"{month}.csv"
        try:
            stat = csv_path.stat()
        except FileNotFoundError:
            return (display_label, False, 0, 0)
        return (display_label, True, int(stat.st_mtime_ns), int(stat.st_size))

    def _populate_statistics_table(self, table: QTableWidget, header: tuple[str, ...], rows: tuple[tuple[str, ...], ...]) -> None:
        display_columns = self._statistics_display_columns(header)
        display_header = self._statistics_display_header(header)
        previous_signal_state = table.blockSignals(True)
        table.setUpdatesEnabled(False)
        try:
            table.setSortingEnabled(False)
            table.clearContents()
            table.setColumnCount(len(display_header))
            table.setHorizontalHeaderLabels(list(display_header))
            table.setRowCount(len(rows))
            for row_index, row in enumerate(rows):
                for display_index, source_index in enumerate(display_columns):
                    raw_value = row[source_index] if source_index < len(row) else ""
                    value = self._statistics_cell_text(header, source_index, raw_value)
                    item = QTableWidgetItem(str(value))
                    item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                    if source_index in {0, 1, 3, 4, 5, 6}:
                        item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    table.setItem(row_index, display_index, item)
        finally:
            table.setUpdatesEnabled(True)
            table.blockSignals(previous_signal_state)
            table.viewport().update()

    @staticmethod
    def _statistics_display_columns(header: list[str] | tuple[str, ...]) -> tuple[int, ...]:
        return tuple(
            column_index
            for column_index, column_name in enumerate(header)
            if column_name not in STATISTICS_HIDDEN_COLUMNS
        )

    def _statistics_display_header(self, header: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            self.t(f"statistics.header.{header[column_index]}")
            for column_index in self._statistics_display_columns(header)
        )

    def _statistics_cell_text(self, header: list[str] | tuple[str, ...], column_index: int, value: str) -> str:
        if column_index < len(header) and header[column_index] == "Completion Status":
            if value == "Complete":
                return self.t("statistics.value.complete")
            if value == "Incomplete":
                return self.t("statistics.value.incomplete")
        return value

    def _statistics_status_text(
        self,
        statistics_key: str,
        display_label: str,
        exists: bool,
        row_count: int,
        total_success_copies: int,
    ) -> str:
        success_text = self.t("statistics.total_success", count=total_success_copies)
        if statistics_key == "today":
            if not exists:
                return f"{display_label} - {self.t('statistics.no_current_month_csv')} - {success_text}"
            if row_count:
                return f"{display_label} - {self.t('statistics.today_rows', count=row_count)} - {success_text}"
            return f"{display_label} - {self.t('statistics.no_today_rows')} - {success_text}"
        if statistics_key == "previous_month":
            if not exists:
                return f"{display_label} - {self.t('statistics.no_previous_month_csv')} - {success_text}"
            if row_count:
                return f"{display_label} - {self.t('statistics.previous_month_rows', count=row_count)} - {success_text}"
            return f"{display_label} - {self.t('statistics.no_previous_month_rows')} - {success_text}"
        if not exists:
            return f"{display_label} - {self.t('statistics.no_current_month_csv')} - {success_text}"
        if row_count:
            return f"{display_label} - {self.t('statistics.current_month_rows', count=row_count)} - {success_text}"
        return f"{display_label} - {self.t('statistics.no_current_month_rows')} - {success_text}"

    def on_log_text(self, text: str, once_key: str | None = None) -> None:
        debug_log(f"app-log {text}")
        if not self.regular_log_filter.should_write(text, once_key=once_key):
            return
        self.log_edit.appendPlainText(text)
        self.log_writer.append_line(text)

    def on_spool_progress(self, sent: int, total: int) -> None:
        self._spool_total = max(0, int(total))
        self.spool_progress_bar.setRange(0, max(1, self._spool_total))
        self.spool_progress_bar.setValue(max(0, int(sent)))
        self._set_spool_progress_text(int(sent))

    def on_task_status(self, status: TaskStatusMessage) -> None:
        task = self.task_service.apply_status(self.tasks, status)
        if task is None:
            return
        self.refresh_task_row(task)
        if status.error_message:
            self.on_log_text(self.t("task.error_log", file_name=task.file_name(), error=status.error_message))

    def refresh_task_row(self, task: TaskItem) -> None:
        row = self.task_row_by_id.get(task.task_id)
        if row is None:
            self.refresh_task_table()
            return
        status_item = self.task_table.item(row, 4)
        if status_item is not None:
            status_item.setText(self._task_status_text(task.status))

    def on_worker_status(self, status: WorkerStatusMessage) -> None:
        row = self.worker_row_by_name.get(status.worker_name)
        if row is None:
            return
        self._set_worker_status_item(row, status.status)

    def on_run_state_changed(self, running: bool) -> None:
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(running)
        self._set_task_editing_enabled(not running)
        if running:
            self.spool_progress_bar.setValue(0)
        self._refresh_start_button_text()
        self.refresh_task_table()
        self.on_log_text(self.t("log.flow_started" if running else "log.flow_ended"))
        if not running and self._statistics_card_context() is not None:
            self.refresh_statistics_table(force=True)

    def on_pause_state_changed(self, paused: bool) -> None:
        if not self.controller.is_running():
            return
        self._refresh_start_button_text()

    def update_task_preview(self) -> None:
        row = self.task_table.currentRow()
        if row < 0 or row >= len(self.tasks):
            self.preview_label.clear()
            self.current_preview_pixmap = None
            return
        task = self.tasks[row]
        if task.preview_path and Path(task.preview_path).exists():
            pixmap = QPixmap(task.preview_path)
            self.current_preview_pixmap = pixmap
            self._apply_preview_pixmap()
        else:
            self.preview_label.clear()
            self.current_preview_pixmap = None

    def _apply_preview_pixmap(self) -> None:
        if self.current_preview_pixmap is None or self.current_preview_pixmap.isNull():
            self.preview_label.clear()
            return
        scaled = self.current_preview_pixmap.scaled(
            self.preview_label.size(),
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        self.preview_label.setPixmap(scaled)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._apply_preview_pixmap()

    def open_print_management(self) -> None:
        try:
            if sys.platform == "win32":
                os.startfile("printmanagement.msc")
            else:
                self.on_log_text(self.t("log.print_mgmt_unsupported"))
        except Exception as exc:
            QMessageBox.warning(self, self.t("dialog.start_failed"), self.t("message.open_print_management_failed", error=exc))

    def open_program_dir(self) -> None:
        self._open_path(self.root_dir)

    def clear_cache_dir(self, log_message: bool = True) -> None:
        self.cache_service.clear()
        if log_message:
            self.on_log_text(self.t("log.cache_cleared"))

    def _open_path(self, path: Path) -> None:
        if sys.platform == "win32":
            os.startfile(str(path))  # type: ignore[attr-defined]
        else:
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(path)))

    def closeEvent(self, event) -> None:
        if self._spooler_maintenance_active:
            QMessageBox.warning(self, self.t("dialog.processing"), self.t("message.cannot_exit_spooler"))
            event.ignore()
            return
        if self.controller.is_running():
            QMessageBox.warning(self, self.t("dialog.run_active"), self.t("message.cannot_exit_running"))
            event.ignore()
            return
        self.save_worker_settings()
        self.app_settings["active_worker_group"] = self.current_worker_group
        self.app_settings["task_default_copies"] = int(self.task_copies_value_box.value())
        self._remember_window_layout()
        self.store.save_app_settings(self.app_settings)
        if self.app_settings.get("save_tasks_on_exit", False):
            self.task_service.save_session(self.tasks)
        else:
            self.task_service.clear_session()
        super().closeEvent(event)


def _qt_platform_arg_from_settings(root_dir: Path) -> str | None:
    store = ConfigStore(root_dir)
    settings = store.load_app_settings()
    engine = str(settings.get("font_engine", "auto") or "auto").lower()
    if sys.platform != "win32":
        return None
    if engine == "gdi":
        return "windows:nodirectwrite,fontengine=gdi"
    if engine == "freetype":
        return "windows:fontengine=freetype"
    return None


def _prepare_qt_runtime() -> None:
    QApplication.setAttribute(Qt.ApplicationAttribute.AA_Use96Dpi, True)
    try:
        QApplication.setHighDpiScaleFactorRoundingPolicy(Qt.HighDpiScaleFactorRoundingPolicy.PassThrough)
    except Exception:
        pass

    platform_arg = _qt_platform_arg_from_settings(get_app_root())
    if platform_arg and "-platform" not in sys.argv:
        sys.argv.extend(["-platform", platform_arg])



def run() -> None:
    root_dir = get_app_root()
    store = ConfigStore(root_dir)
    initialize_debug_logging(store.paths.logs_dir)
    debug_log(f"run() starting root_dir={root_dir}")
    _prepare_qt_runtime()
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    install_qt_message_handler()
    debug_log(f"QApplication started argv={sys.argv}")
    language = "en"
    try:
        language = normalize_language(store.load_app_settings().get("language", "en"))
        PrintController.validate_environment(language=language)
    except Exception as exc:
        debug_exception("run.environment", exc)
        QMessageBox.critical(None, translate(language, "message.environment_check_failed"), str(exc))
        sys.exit(1)
    window = MainWindow()
    window.show()
    debug_log("mainwindow shown")
    sys.exit(app.exec())
