from __future__ import annotations

from pathlib import Path
import copy
import os
import shutil
import sys
import ctypes


from PySide6.QtCore import Qt, QUrl, QSize, QTimer
from PySide6.QtGui import QAction, QDesktopServices, QFont, QIcon, QPixmap
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
    QInputDialog,
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
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from .config_store import ConfigStore
from .controller import PrintController
from .local_logger import LocalLogWriter
from .models import RunOptions, SUPPORTED_INPUT_SUFFIXES, TaskItem, TaskStatusMessage, WorkerConfig, WorkerStatusMessage
from .printui import (
    open_printer_preferences,
    open_printer_properties,
    restore_printer_settings,
    save_printer_settings,
)
from .task_inspector import TaskInspectionError, build_preview_file, inspect_task_input
from .debug_logger import debug_exception, debug_log, initialize_debug_logging, install_qt_message_handler


APP_NAME = "InkSwarm"
APP_VERSION = "0.1.4"
DEBUG_LOG_NAME = "debug.log"


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
            if path.suffix.lower() in SUPPORTED_INPUT_SUFFIXES:
                files.append(path)
        if files:
            self.on_files_dropped(files)
            event.acceptProposedAction()
        else:
            event.ignore()


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} {APP_VERSION}")
        self.resize(1450, 940)

        self.root_dir = get_app_root()
        self._apply_app_icon()
        self.store = ConfigStore(self.root_dir)
        self.controller = PrintController(self.store.paths.cache_dir, self.store.paths.statistics_dir)
        self.controller.signals.log.connect(self.on_log)
        self.controller.signals.task_status.connect(self.on_task_status)
        self.controller.signals.worker_status.connect(self.on_worker_status)
        self.controller.signals.run_state.connect(self.on_run_state_changed)
        self.controller.signals.spool_progress.connect(self.on_spool_progress)
        self.log_writer = LocalLogWriter(self.store.paths.logs_dir)
        self.debug_log_path = (self.store.paths.logs_dir / DEBUG_LOG_NAME).resolve()
        debug_log(f"mainwindow init root_dir={self.root_dir}")
        self.app_settings = self.store.load_app_settings()
        debug_log(f"settings loaded {self.app_settings}")
        self.set_console_visibility(False)
        self.current_worker_group = self.app_settings.get("active_worker_group", self.store.default_group_dir().name)

        self.tasks: list[TaskItem] = []
        self.task_row_by_id: dict[str, int] = {}
        self.workers: list[WorkerConfig] = []
        self.worker_row_by_name: dict[str, int] = {}
        self.current_preview_pixmap: QPixmap | None = None
        self._spool_total = 0

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
            self.on_log_text("已自动清理上次缓存。")
        QTimer.singleShot(0, self.apply_saved_startup_ui_state)

    def _add_section_header(self, layout: QVBoxLayout, title: str) -> None:
        layout.addWidget(QLabel(title))
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setFrameShadow(QFrame.Sunken)
        layout.addWidget(line)

    def _build_ui(self) -> None:
        self._build_menu_bar()

        central = QWidget()
        layout = QVBoxLayout(central)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self.main_splitter = QSplitter(Qt.Vertical)

        top = QFrame()
        top.setObjectName("sectionPanel")
        top_layout = QVBoxLayout(top)
        top_layout.setContentsMargins(8, 8, 8, 8)
        top_layout.setSpacing(6)
        self._add_section_header(top_layout, "任务列表（支持拖放 PDF / 图片）")

        self.top_splitter = QSplitter(Qt.Horizontal)

        self.task_table = FileDropTable(self.add_files)
        self.task_table.setColumnCount(5)
        self.task_table.setHorizontalHeaderLabels(["文件", "份数", "打印尺寸", "状态", "分配"])
        self.task_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.task_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.task_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.task_table.setAlternatingRowColors(True)
        self.task_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.task_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Fixed)
        self.task_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Fixed)
        self.task_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Fixed)
        self.task_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.task_table.setColumnWidth(1, 88)
        self.task_table.setColumnWidth(2, 160)
        self.task_table.setColumnWidth(3, 160)
        self.task_table.itemSelectionChanged.connect(self.update_task_preview)
        self.top_splitter.addWidget(self.task_table)

        preview_panel = QWidget()
        preview_panel.setMinimumHeight(0)
        preview_panel.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Ignored)
        preview_layout = QVBoxLayout(preview_panel)
        preview_layout.setContentsMargins(0, 0, 0, 0)
        preview_layout.setSpacing(0)
        self.preview_label = QLabel()
        self.preview_label.setMinimumWidth(240)
        self.preview_label.setMinimumHeight(0)
        self.preview_label.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Ignored)
        self.preview_label.setAlignment(Qt.AlignCenter)
        self.preview_label.setScaledContents(False)
        self.preview_label.setStyleSheet("border: 1px solid palette(mid); background: palette(base);")
        preview_layout.addWidget(self.preview_label, 1)
        self.top_splitter.addWidget(preview_panel)
        self.top_splitter.setSizes([1120, 260])
        self.top_splitter.setStretchFactor(0, 4)
        self.top_splitter.setStretchFactor(1, 1)
        top_layout.addWidget(self.top_splitter, 1)

        task_buttons = QHBoxLayout()
        btn_add = QPushButton("添加文件")
        btn_add.clicked.connect(self.pick_files)
        btn_remove = QPushButton("移除选中")
        btn_remove.clicked.connect(self.remove_selected_tasks)
        btn_clear = QPushButton("清空任务")
        btn_clear.clicked.connect(self.clear_tasks)
        btn_copies = QPushButton("批量设置份数")
        btn_copies.clicked.connect(self.set_selected_task_copies)
        for btn in [btn_add, btn_remove, btn_clear, btn_copies]:
            task_buttons.addWidget(btn)
        task_buttons.addStretch(1)
        top_layout.addLayout(task_buttons)

        self.bottom_splitter = QSplitter(Qt.Vertical)

        worker_panel = QFrame()
        worker_panel.setObjectName("sectionPanel")
        worker_layout = QVBoxLayout(worker_panel)
        worker_layout.setContentsMargins(8, 8, 8, 8)
        worker_layout.setSpacing(6)
        self._add_section_header(worker_layout, "Worker 列表")

        worker_content = QHBoxLayout()
        worker_content.setSpacing(8)

        worker_left = QWidget()
        worker_left_layout = QVBoxLayout(worker_left)
        worker_left_layout.setContentsMargins(0, 0, 0, 0)
        worker_left_layout.setSpacing(6)

        self.worker_table = QTableWidget()
        self.worker_table.setColumnCount(6)
        self.worker_table.setHorizontalHeaderLabels(["启用", "Worker", "打印机", "预设", "速度", "状态"])
        self.worker_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.worker_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.worker_table.setAlternatingRowColors(True)
        header = self.worker_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Fixed)
        header.setSectionResizeMode(1, QHeaderView.Fixed)
        header.setSectionResizeMode(2, QHeaderView.Fixed)
        header.setSectionResizeMode(3, QHeaderView.Fixed)
        header.setSectionResizeMode(4, QHeaderView.Fixed)
        header.setSectionResizeMode(5, QHeaderView.Stretch)
        self.worker_table.setColumnWidth(0, 56)
        self.worker_table.setColumnWidth(1, 150)
        self.worker_table.setColumnWidth(2, 150)
        self.worker_table.setColumnWidth(3, 225)
        self.worker_table.setColumnWidth(4, 82)
        worker_left_layout.addWidget(self.worker_table, 1)

        worker_buttons = QHBoxLayout()
        worker_buttons.addWidget(QLabel("方案组"))
        self.worker_group_combo = QComboBox()
        self.worker_group_combo.setMinimumWidth(150)
        self.worker_group_combo.currentIndexChanged.connect(self.on_worker_group_changed)
        worker_buttons.addWidget(self.worker_group_combo)

        btn_reload_workers = QPushButton("重载 Worker")
        btn_reload_workers.clicked.connect(self.reload_workers)
        btn_save_workers = QPushButton("保存 Worker 设定")
        btn_save_workers.clicked.connect(self.save_worker_settings)
        btn_open_pref = QPushButton("打开驱动首选项")
        btn_open_pref.clicked.connect(self.open_selected_worker_preferences)
        btn_open_props = QPushButton("打开打印机属性")
        btn_open_props.clicked.connect(self.open_selected_worker_properties)
        btn_capture = QPushButton("导出当前驱动设定")
        btn_capture.clicked.connect(self.capture_selected_worker_snapshot)
        for btn in [btn_reload_workers, btn_save_workers, btn_open_pref, btn_open_props, btn_capture]:
            worker_buttons.addWidget(btn)
        worker_buttons.addStretch(1)
        worker_left_layout.addLayout(worker_buttons)

        controls_panel = QFrame()
        controls_panel.setObjectName("workerControlPanel")
        controls_panel.setMinimumWidth(180)
        controls_panel.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)
        controls_layout = QVBoxLayout(controls_panel)
        controls_layout.setContentsMargins(10, 10, 10, 10)
        controls_layout.setSpacing(10)

        self.start_button = QPushButton("开始发送")
        self.start_button.clicked.connect(self.start_run)
        self.start_button.setObjectName("primaryActionButton")
        self.start_button.setMinimumHeight(96)
        self.start_button.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        self.stop_button = QPushButton("停止")
        self.stop_button.clicked.connect(self.stop_run)
        self.stop_button.setObjectName("dangerActionButton")
        self.stop_button.setEnabled(False)
        self.stop_button.setMinimumHeight(96)
        self.stop_button.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        controls_layout.addWidget(self.start_button)
        controls_layout.addWidget(self.stop_button)

        self.spool_progress_bar = QProgressBar()
        self.spool_progress_bar.setRange(0, 1)
        self.spool_progress_bar.setValue(0)
        self.spool_progress_bar.setTextVisible(True)
        self.spool_progress_bar.setFormat("已发送到 Spooler: 0 / 0")
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
        self._add_section_header(log_layout, "日志")
        self.log_edit = QPlainTextEdit()
        self.log_edit.setReadOnly(True)
        log_layout.addWidget(self.log_edit, 1)

        self.bottom_splitter.addWidget(worker_panel)
        self.bottom_splitter.addWidget(log_panel)
        self.bottom_splitter.setSizes([470, 210])
        self.bottom_splitter.setStretchFactor(0, 3)
        self.bottom_splitter.setStretchFactor(1, 2)

        self.main_splitter.addWidget(top)
        self.main_splitter.addWidget(self.bottom_splitter)
        self.main_splitter.setSizes([320, 620])
        self.main_splitter.setStretchFactor(0, 2)
        self.main_splitter.setStretchFactor(1, 3)

        layout.addWidget(self.main_splitter)
        self.setCentralWidget(central)

    def _build_menu_bar(self) -> None:
        menu = self.menuBar()
        settings_action = QAction("设置", self)
        settings_action.triggered.connect(self.open_settings_dialog)
        menu.addAction(settings_action)

        open_root_action = QAction("打开程序主目录", self)
        open_root_action.triggered.connect(self.open_program_dir)
        menu.addAction(open_root_action)

        print_mgmt_action = QAction("启动打印管理器", self)
        print_mgmt_action.triggered.connect(self.open_print_management)
        menu.addAction(print_mgmt_action)

        help_action = QAction("帮助", self)
        help_action.triggered.connect(self.open_help_dialog)
        menu.addAction(help_action)

    def open_settings_dialog(self) -> None:
        dialog = QDialog(self)
        dialog.setWindowTitle("设置")
        dialog.setModal(True)
        dialog.resize(520, 460)

        layout = QVBoxLayout(dialog)
        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignRight | Qt.AlignVCenter)

        autoclear_checkbox = QCheckBox("启动时自动清理上次缓存")
        autoclear_checkbox.setChecked(bool(self.app_settings.get("auto_clear_cache_on_start", False)))
        form.addRow("缓存", autoclear_checkbox)

        save_tasks_checkbox = QCheckBox("退出时保存任务列表")
        save_tasks_checkbox.setChecked(bool(self.app_settings.get("save_tasks_on_exit", False)))
        form.addRow("任务", save_tasks_checkbox)

        font_edit = QLineEdit(str(self.app_settings.get("font_family", "Segoe UI") or "Segoe UI"))
        font_edit.setClearButtonEnabled(True)
        form.addRow("字体", font_edit)

        scale_combo = QComboBox()
        for value in [100, 125, 150, 175, 200]:
            scale_combo.addItem(f"{value}%", value)
        current_scale = int(self.app_settings.get("ui_scale", 100))
        idx = scale_combo.findData(current_scale)
        if idx >= 0:
            scale_combo.setCurrentIndex(idx)
        form.addRow("界面缩放", scale_combo)

        font_engine_combo = QComboBox()
        font_engine_combo.addItem("Auto", "auto")
        font_engine_combo.addItem("GDI（关闭 DirectWrite）", "gdi")
        font_engine_combo.addItem("FreeType", "freetype")
        current_engine = str(self.app_settings.get("font_engine", "auto") or "auto").lower()
        engine_idx = font_engine_combo.findData(current_engine)
        if engine_idx >= 0:
            font_engine_combo.setCurrentIndex(engine_idx)
        form.addRow("字体引擎（重启生效）", font_engine_combo)

        ignore_margins_checkbox = QCheckBox("打印时尽量满版，不为页边距让位")
        ignore_margins_checkbox.setChecked(bool(self.app_settings.get("ignore_margins", True)))
        form.addRow("忽略页边距", ignore_margins_checkbox)

        orient_enabled_checkbox = QCheckBox("启用")
        orient_enabled_checkbox.setChecked(bool(self.app_settings.get("auto_orient_enabled", False)))
        form.addRow("自适应纸张方向", orient_enabled_checkbox)

        orientation_combo = QComboBox()
        orientation_combo.addItem("Portrait", "portrait")
        orientation_combo.addItem("Landscape", "landscape")
        orient_idx = orientation_combo.findData(str(self.app_settings.get("target_orientation", "portrait") or "portrait").lower())
        if orient_idx >= 0:
            orientation_combo.setCurrentIndex(orient_idx)
        orientation_combo.setEnabled(orient_enabled_checkbox.isChecked())
        orient_enabled_checkbox.toggled.connect(orientation_combo.setEnabled)
        form.addRow("目标方向", orientation_combo)

        queue_limit_enabled_checkbox = QCheckBox("启用")
        queue_limit_enabled_checkbox.setChecked(bool(self.app_settings.get("worker_queue_limit_enabled", False)))
        form.addRow("Worker 最大排队数", queue_limit_enabled_checkbox)

        queue_limit_spin = QSpinBox()
        queue_limit_spin.setRange(1, 999)
        queue_limit_spin.setValue(int(self.app_settings.get("worker_queue_limit", 3) or 3))
        queue_limit_spin.setEnabled(queue_limit_enabled_checkbox.isChecked())
        queue_limit_enabled_checkbox.toggled.connect(queue_limit_spin.setEnabled)
        form.addRow("最大排队值", queue_limit_spin)

        rip_limit_enabled_checkbox = QCheckBox("启用")
        rip_limit_enabled_checkbox.setChecked(bool(self.app_settings.get("rip_limit_enabled", True)))
        form.addRow("RIP 精度限制", rip_limit_enabled_checkbox)

        rip_limit_spin = QSpinBox()
        rip_limit_spin.setRange(72, 1200)
        rip_limit_spin.setSingleStep(25)
        rip_limit_spin.setValue(int(self.app_settings.get("rip_limit_ppi", 300) or 300))
        rip_limit_spin.setEnabled(rip_limit_enabled_checkbox.isChecked())
        rip_limit_enabled_checkbox.toggled.connect(rip_limit_spin.setEnabled)
        form.addRow("最大 PPI", rip_limit_spin)

        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        if dialog.exec() != QDialog.Accepted:
            return

        new_autoclear = bool(autoclear_checkbox.isChecked())
        new_save_tasks = bool(save_tasks_checkbox.isChecked())
        new_font = font_edit.text().strip() or "Segoe UI"
        new_scale = int(scale_combo.currentData())

        self.app_settings["auto_clear_cache_on_start"] = new_autoclear
        self.app_settings["save_tasks_on_exit"] = new_save_tasks
        self.app_settings["font_family"] = new_font
        self.app_settings["ui_scale"] = new_scale
        self.app_settings["font_engine"] = str(font_engine_combo.currentData() or "auto")
        self.app_settings["ignore_margins"] = bool(ignore_margins_checkbox.isChecked())
        self.app_settings["auto_orient_enabled"] = bool(orient_enabled_checkbox.isChecked())
        self.app_settings["target_orientation"] = str(orientation_combo.currentData())
        self.app_settings["worker_queue_limit_enabled"] = bool(queue_limit_enabled_checkbox.isChecked())
        self.app_settings["worker_queue_limit"] = int(queue_limit_spin.value())
        self.app_settings["rip_limit_enabled"] = bool(rip_limit_enabled_checkbox.isChecked())
        self.app_settings["rip_limit_ppi"] = int(rip_limit_spin.value())
        self.store.save_app_settings(self.app_settings)
        if not new_save_tasks:
            self.store.clear_task_session()
        self.apply_ui_scale(new_scale)

    def open_help_dialog(self) -> None:
        QMessageBox.information(
            self,
            f"关于 {APP_NAME}",
            f"{APP_NAME} {APP_VERSION}\n\n"
            "软件逻辑：\n"
            "将文件列表中的每个条目视为一个任务，先按 Worker 的速度比例分配总张数，再按轮询顺序投递到各台打印机。相同 Worker 对同一任务只会进行一次渲染与色彩转换，并复用缓存结果连续发送。可选的纸张方向自适应会在发送前统一旋转到 Portrait 或 Landscape。\n\n"
            "基本流程：\n"
            "1. 准备 Workers 目录组、打印机与 preset。\n"
            "2. 根据需要为 preset 绑定 ICC 与驱动快照。\n"
            "3. 拖入 PDF 或图片，设置每个任务的份数。\n"
            "4. 启用需要参与工作的 Worker，确认速度和预设。\n"
            "5. 开始发送，由调度器并行分发到各 Worker。\n\n"
            "开发者：hyyz172000@gmail.com",
        )

    def _apply_app_icon(self) -> None:
        for name in ("app.ico", "inkswarm.ico"):
            icon_path = self.root_dir / name
            if icon_path.exists():
                icon = QIcon(str(icon_path))
                self.setWindowIcon(icon)
                app = QApplication.instance()
                if app is not None:
                    app.setWindowIcon(icon)
                break

    def _base_font(self) -> QFont:
        family = str(self.app_settings.get("font_family", "Segoe UI") or "Segoe UI")
        font = QFont(family)
        point_size = font.pointSizeF()
        if point_size <= 0:
            point_size = 9.0
        font.setPointSizeF(point_size)
        font.setStyleStrategy(QFont.PreferAntialias)
        try:
            font.setHintingPreference(QFont.PreferFullHinting)
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

    def apply_ui_scale(self, scale: int) -> None:
        app = QApplication.instance()
        if app is not None:
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
        self.task_table.setColumnWidth(1, max(82, round(88 * scale / 100)))
        status_width = max(145, round(160 * scale / 100))
        self.task_table.setColumnWidth(2, status_width)
        self.task_table.setColumnWidth(3, status_width)
        self.top_splitter.setSizes([max(860, round(1120 * scale / 100)), max(220, round(260 * scale / 100))])
        self.bottom_splitter.setSizes([max(390, round(470 * scale / 100)), max(180, round(210 * scale / 100))])
        self.main_splitter.setSizes([max(280, round(320 * scale / 100)), max(520, round(620 * scale / 100))])
        self.update_task_preview()

    def apply_saved_startup_ui_state(self) -> None:
        if self._ui_scale_applied_once:
            return
        self._ui_scale_applied_once = True
        self.apply_ui_scale(self._saved_ui_scale)
        self.set_console_visibility(False)

    @staticmethod
    def _display_worker_group_name(group_name: str) -> str:
        if group_name == "Workers":
            return "默认"
        if group_name == "workers":
            return "默认(legacy)"
        if group_name.startswith("Workers_"):
            return group_name[len("Workers_"):] or "默认"
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
        groups = self.store.list_worker_groups()
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
        session_items = self.store.load_task_session()
        if not session_items:
            return
        files_to_add: list[Path] = []
        copies_map: dict[Path, int] = {}
        for item in session_items:
            raw_path = item.get("file_path")
            if not raw_path:
                continue
            path = Path(raw_path)
            if path.exists() and path.suffix.lower() in SUPPORTED_INPUT_SUFFIXES:
                files_to_add.append(path)
                copies_map[path.resolve()] = max(1, int(item.get("copies", 1)))
        self.add_files(files_to_add)
        for task in self.tasks:
            task.copies = copies_map.get(task.file_path.resolve(), task.copies)
        self.refresh_task_table()
        if files_to_add:
            self.on_log_text(f"已恢复 {len(files_to_add)} 个上次任务。")

    def pick_files(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "选择打印文件",
            str(self.root_dir),
            "Supported Files (*.pdf *.jpg *.jpeg *.png *.tif *.tiff *.bmp)",
        )
        self.add_files([Path(f) for f in files])

    def add_files(self, files: list[Path]) -> None:
        existing_paths = {task.file_path.resolve() for task in self.tasks}
        added_count = 0
        for path in files:
            if not path.exists() or path.suffix.lower() not in SUPPORTED_INPUT_SUFFIXES:
                continue
            resolved = path.resolve()
            if resolved in existing_paths:
                continue
            try:
                inspection = inspect_task_input(resolved)
            except TaskInspectionError as exc:
                self.on_log_text(f"跳过 {resolved.name}: {exc}")
                continue
            task = TaskItem(file_path=resolved, display_size_mm=inspection.display_size_mm)
            preview_path = build_preview_file(self.store.paths.preview_dir, task.task_id, inspection.preview_bytes)
            task.preview_path = str(preview_path)
            self.tasks.append(task)
            existing_paths.add(resolved)
            added_count += 1
        self.refresh_task_table()
        if added_count:
            self.on_log_text(f"已添加 {added_count} 个任务。")

    def refresh_task_table(self) -> None:
        self.task_table.setRowCount(len(self.tasks))
        self.task_row_by_id.clear()
        for row, task in enumerate(self.tasks):
            self.task_row_by_id[task.task_id] = row

            file_item = QTableWidgetItem(task.file_name())
            file_item.setData(Qt.UserRole, task.task_id)
            file_item.setFlags(file_item.flags() & ~Qt.ItemIsEditable)
            self.task_table.setItem(row, 0, file_item)

            copies_box = QSpinBox()
            copies_box.setRange(1, 9999)
            copies_box.setAlignment(Qt.AlignCenter)
            copies_box.setValue(task.copies)
            copies_box.valueChanged.connect(lambda value, task_id=task.task_id: self.on_task_copies_changed(task_id, value))
            self.task_table.setCellWidget(row, 1, copies_box)

            size_item = QTableWidgetItem(task.display_size_mm)
            size_item.setTextAlignment(Qt.AlignCenter)
            size_item.setFlags(size_item.flags() & ~Qt.ItemIsEditable)
            self.task_table.setItem(row, 2, size_item)

            status_item = QTableWidgetItem(task.status)
            status_item.setTextAlignment(Qt.AlignCenter)
            status_item.setFlags(status_item.flags() & ~Qt.ItemIsEditable)
            self.task_table.setItem(row, 3, status_item)

            assigned_item = QTableWidgetItem(task.assigned_summary)
            assigned_item.setFlags(assigned_item.flags() & ~Qt.ItemIsEditable)
            self.task_table.setItem(row, 4, assigned_item)

        self.update_task_preview()

    def on_task_copies_changed(self, task_id: str, value: int) -> None:
        task = next((t for t in self.tasks if t.task_id == task_id), None)
        if task is not None:
            task.copies = int(value)

    def remove_selected_tasks(self) -> None:
        rows = sorted({index.row() for index in self.task_table.selectedIndexes()}, reverse=True)
        for row in rows:
            self.tasks.pop(row)
        self.refresh_task_table()

    def clear_tasks(self) -> None:
        if self.controller.is_running():
            QMessageBox.warning(self, "运行中", "请先停止当前流程。")
            return
        self.tasks.clear()
        self.refresh_task_table()

    def set_selected_task_copies(self) -> None:
        rows = sorted({index.row() for index in self.task_table.selectedIndexes()})
        if not rows:
            return
        value, ok = QInputDialog.getInt(self, "份数", "输入份数", value=1, minValue=1, maxValue=9999)
        if not ok:
            return
        for row in rows:
            self.tasks[row].copies = value
        self.refresh_task_table()

    def _centered_widget(self, child: QWidget) -> QWidget:
        wrapper = QWidget()
        layout = QHBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setAlignment(Qt.AlignCenter)
        layout.addWidget(child)
        return wrapper

    def reload_workers(self) -> None:
        self.workers = self.store.load_workers(self.current_worker_group)
        self.worker_table.setRowCount(len(self.workers))
        self.worker_row_by_name.clear()
        for row, worker in enumerate(self.workers):
            self.worker_row_by_name[worker.name] = row

            enabled_box = QCheckBox()
            enabled_box.setChecked(worker.enabled)
            self.worker_table.setCellWidget(row, 0, self._centered_widget(enabled_box))

            name_item = QTableWidgetItem(worker.name)
            name_item.setFlags(name_item.flags() & ~Qt.ItemIsEditable)
            self.worker_table.setItem(row, 1, name_item)

            printer_item = QTableWidgetItem(worker.printer_name)
            self.worker_table.setItem(row, 2, printer_item)

            preset_combo = QComboBox()
            preset_names = sorted(worker.presets.keys())
            for preset_name in preset_names:
                preset_combo.addItem(preset_name)
            active_index = preset_combo.findText(worker.active_preset)
            if active_index >= 0:
                preset_combo.setCurrentIndex(active_index)
            self.worker_table.setCellWidget(row, 3, preset_combo)

            weight_box = QSpinBox()
            weight_box.setRange(1, 100)
            weight_box.setAlignment(Qt.AlignCenter)
            weight_box.setValue(worker.weight)
            self.worker_table.setCellWidget(row, 4, weight_box)

            status_item = QTableWidgetItem("Idle")
            status_item.setTextAlignment(Qt.AlignCenter)
            status_item.setFlags(status_item.flags() & ~Qt.ItemIsEditable)
            self.worker_table.setItem(row, 5, status_item)

        self.on_log_text(f"已加载方案组 {self.current_worker_group}，共 {len(self.workers)} 个 Worker。")

    def save_worker_settings(self) -> None:
        for row, worker in enumerate(self.workers):
            enabled_wrapper = self.worker_table.cellWidget(row, 0)
            enabled_box = enabled_wrapper.findChild(QCheckBox) if enabled_wrapper else None
            preset_combo = self.worker_table.cellWidget(row, 3)
            weight_box = self.worker_table.cellWidget(row, 4)
            worker.enabled = bool(enabled_box.isChecked()) if enabled_box is not None else worker.enabled
            printer_item = self.worker_table.item(row, 2)
            worker.printer_name = printer_item.text().strip() if printer_item is not None else worker.printer_name
            worker.active_preset = preset_combo.currentText() if preset_combo is not None else worker.active_preset
            worker.weight = int(weight_box.value()) if weight_box is not None else worker.weight
        self.store.save_workers(self.workers)
        self.on_log_text("Worker 配置已保存。")

    def _selected_worker(self) -> WorkerConfig | None:
        row = self.worker_table.currentRow()
        if row < 0 or row >= len(self.workers):
            return None
        self.save_worker_settings()
        return self.workers[row]

    def _restore_worker_preset_if_any(self, worker: WorkerConfig) -> None:
        preset = worker.get_active_preset()
        snapshot_path = worker.resolve_path(preset.printui_restore_file) if preset.printui_restore_file else None
        if snapshot_path and snapshot_path.exists():
            restore_printer_settings(worker.printer_name, snapshot_path)
            self.on_log_text(f"已载入 {worker.name}/{preset.name} 的驱动快照。")

    def open_selected_worker_preferences(self) -> None:
        worker = self._selected_worker()
        if worker is None:
            QMessageBox.information(self, "提示", "请先选中一个 Worker。")
            return
        if not worker.printer_name:
            QMessageBox.warning(self, "提示", "该 Worker 还没有填写打印机名称。")
            return
        try:
            self._restore_worker_preset_if_any(worker)
            open_printer_preferences(worker.printer_name)
        except Exception as exc:
            QMessageBox.critical(self, "打开失败", str(exc))

    def open_selected_worker_properties(self) -> None:
        worker = self._selected_worker()
        if worker is None:
            QMessageBox.information(self, "提示", "请先选中一个 Worker。")
            return
        if not worker.printer_name:
            QMessageBox.warning(self, "提示", "该 Worker 还没有填写打印机名称。")
            return
        try:
            open_printer_properties(worker.printer_name)
        except Exception as exc:
            QMessageBox.critical(self, "打开失败", str(exc))

    def capture_selected_worker_snapshot(self) -> None:
        worker = self._selected_worker()
        if worker is None:
            QMessageBox.information(self, "提示", "请先选中一个 Worker。")
            return
        if not worker.printer_name:
            QMessageBox.warning(self, "提示", "该 Worker 还没有填写打印机名称。")
            return
        preset = worker.get_active_preset()
        snapshot_path = worker.resolve_path(preset.printui_restore_file) if preset.printui_restore_file else None
        if snapshot_path is None:
            snapshot_path = worker.directory / f"{preset.name}.dat"
            preset.printui_restore_file = snapshot_path.name
        try:
            save_printer_settings(worker.printer_name, snapshot_path)
            self.store.save_worker(worker)
            self.on_log_text(f"已导出 {worker.name}/{preset.name} 的驱动快照: {snapshot_path.name}")
        except Exception as exc:
            QMessageBox.critical(self, "导出失败", str(exc))

    def start_run(self) -> None:
        if self.controller.is_running():
            QMessageBox.warning(self, "运行中", "当前已有流程在运行。")
            return
        if not self.tasks:
            QMessageBox.information(self, "提示", "请先添加任务。")
            return
        self.save_worker_settings()
        tasks = copy.deepcopy(self.tasks)
        workers = copy.deepcopy(self.workers)
        for task in self.tasks:
            task.status = "Waiting"
            task.completed_copies = 0
            task.assigned_summary = ""
            task.error_message = ""
        self.refresh_task_table()
        self._spool_total = sum(max(0, int(task.copies)) for task in tasks)
        self.spool_progress_bar.setRange(0, max(1, self._spool_total))
        self.spool_progress_bar.setValue(0)
        self.spool_progress_bar.setFormat(f"已发送到 Spooler: 0 / {self._spool_total}")
        run_options = RunOptions(
            auto_orient_enabled=bool(self.app_settings.get("auto_orient_enabled", False)),
            target_orientation=str(self.app_settings.get("target_orientation", "portrait") or "portrait").lower(),
            ignore_margins=bool(self.app_settings.get("ignore_margins", True)),
            worker_queue_limit_enabled=bool(self.app_settings.get("worker_queue_limit_enabled", False)),
            worker_queue_limit=int(self.app_settings.get("worker_queue_limit", 3) or 3),
            rip_limit_enabled=bool(self.app_settings.get("rip_limit_enabled", True)),
            rip_limit_ppi=int(self.app_settings.get("rip_limit_ppi", 300) or 300),
        )
        debug_log(f"start_run with options={run_options} tasks={[(t.file_name(), t.copies) for t in tasks]}")
        try:
            self.controller.start(tasks, workers, run_options)
        except Exception as exc:
            QMessageBox.critical(self, "启动失败", str(exc))

    def stop_run(self) -> None:
        if not self.controller.is_running():
            return
        self.controller.stop()

    def on_log(self, message) -> None:
        self.on_log_text(message.format())

    def on_log_text(self, text: str) -> None:
        self.log_edit.appendPlainText(text)
        self.log_writer.append_line(text)
        debug_log(f"app-log {text}")

    def on_spool_progress(self, sent: int, total: int) -> None:
        self._spool_total = max(0, int(total))
        self.spool_progress_bar.setRange(0, max(1, self._spool_total))
        self.spool_progress_bar.setValue(max(0, int(sent)))
        self.spool_progress_bar.setFormat(f"已发送到 Spooler: {int(sent)} / {self._spool_total}")

    def on_task_status(self, status: TaskStatusMessage) -> None:
        task = next((t for t in self.tasks if t.task_id == status.task_id), None)
        if task is None:
            return
        task.status = status.status
        if status.completed_copies is not None:
            task.completed_copies = status.completed_copies
        if status.assigned_summary is not None:
            task.assigned_summary = status.assigned_summary
        if status.error_message is not None:
            task.error_message = status.error_message
        self.refresh_task_row(task)
        if status.error_message:
            self.on_log_text(f"任务 {task.file_name()} 错误: {status.error_message}")

    def refresh_task_row(self, task: TaskItem) -> None:
        row = self.task_row_by_id.get(task.task_id)
        if row is None:
            self.refresh_task_table()
            return
        status_item = self.task_table.item(row, 3)
        if status_item is not None:
            status_item.setText(task.status)
        assigned_item = self.task_table.item(row, 4)
        if assigned_item is not None:
            assigned_item.setText(task.assigned_summary)

    def on_worker_status(self, status: WorkerStatusMessage) -> None:
        row = self.worker_row_by_name.get(status.worker_name)
        if row is None:
            return
        item = self.worker_table.item(row, 5)
        if item is None:
            item = QTableWidgetItem()
            self.worker_table.setItem(row, 5, item)
        item.setText(status.status)

    def on_run_state_changed(self, running: bool) -> None:
        self.start_button.setEnabled(not running)
        self.stop_button.setEnabled(running)
        if running:
            self.spool_progress_bar.setValue(0)
        self.on_log_text("流程已启动。" if running else "流程已结束。")

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
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation,
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
                self.on_log_text("当前平台不支持启动 printmanagement.msc。")
        except Exception as exc:
            QMessageBox.warning(self, "启动失败", f"无法启动打印管理器：{exc}")

    def open_program_dir(self) -> None:
        self._open_path(self.root_dir)

    def clear_cache_dir(self, log_message: bool = True) -> None:
        if self.store.paths.cache_dir.exists():
            shutil.rmtree(self.store.paths.cache_dir, ignore_errors=True)
        self.store.paths.cache_dir.mkdir(parents=True, exist_ok=True)
        self.store.paths.preview_dir.mkdir(parents=True, exist_ok=True)
        if log_message:
            self.on_log_text("缓存目录已清理。")

    def _open_path(self, path: Path) -> None:
        if sys.platform == "win32":
            os.startfile(str(path))  # type: ignore[attr-defined]
        else:
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(path)))

    def closeEvent(self, event) -> None:
        if self.controller.is_running():
            QMessageBox.warning(self, "运行中", "请先停止当前流程后再退出。")
            event.ignore()
            return
        self.save_worker_settings()
        self.app_settings["active_worker_group"] = self.current_worker_group
        self.store.save_app_settings(self.app_settings)
        if self.app_settings.get("save_tasks_on_exit", False):
            self.store.save_task_session(self.tasks)
        else:
            self.store.clear_task_session()
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
    QApplication.setAttribute(Qt.AA_Use96Dpi, True)
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
    window = MainWindow()
    window.show()
    debug_log("mainwindow shown")
    sys.exit(app.exec())
