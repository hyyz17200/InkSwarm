from __future__ import annotations

import ast
from pathlib import Path
import re
from unittest import TestCase

from printfarm.gui import MainWindow
from printfarm.i18n import DEFAULT_LANGUAGE, _TRANSLATIONS, translate
from printfarm.task_service import SkippedTaskInput


_CJK_RE = re.compile(r"[\u3000-\u303f\u3400-\u9fff\uff00-\uffef]")


def _literal_translation_keys(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.IfExp):
        return _literal_translation_keys(node.body) + _literal_translation_keys(node.orelse)
    return []


class GuiLocalizationTests(TestCase):
    def test_translation_tables_have_matching_keys(self) -> None:
        base_keys = set(_TRANSLATIONS[DEFAULT_LANGUAGE])

        for language, translations in _TRANSLATIONS.items():
            self.assertSetEqual(set(translations), base_keys, language)

    def test_default_fallback_language_stays_english(self) -> None:
        polluted = {
            key: text
            for key, text in _TRANSLATIONS[DEFAULT_LANGUAGE].items()
            if _CJK_RE.search(text)
        }

        self.assertEqual(polluted, {})

    def test_static_translation_keys_used_by_code_exist(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        base_keys = set(_TRANSLATIONS[DEFAULT_LANGUAGE])
        missing: list[str] = []

        for path in (project_root / "printfarm").glob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                if isinstance(node.func, ast.Attribute) and node.func.attr == "t" and node.args:
                    keys = _literal_translation_keys(node.args[0])
                elif isinstance(node.func, ast.Name) and node.func.id == "translate" and len(node.args) >= 2:
                    keys = _literal_translation_keys(node.args[1])
                else:
                    continue
                missing.extend(f"{path.relative_to(project_root)}:{key}" for key in keys if key not in base_keys)

        self.assertEqual(missing, [])

    def test_orientation_labels_can_localize(self) -> None:
        self.assertEqual(translate("en", "settings.orientation.portrait"), "Portrait")
        self.assertEqual(translate("en", "settings.orientation.landscape"), "Landscape")
        self.assertEqual(translate("zh-Hans", "settings.orientation.portrait"), "纵向")
        self.assertEqual(translate("zh-Hans", "settings.orientation.landscape"), "横向")

    def test_settings_dialog_option_labels_can_localize(self) -> None:
        self.assertEqual(
            translate("en", "file_dialog.supported_files"),
            "Supported Files (*.pdf *.jpg *.jpeg *.png *.tif *.tiff *.bmp)",
        )
        self.assertEqual(
            translate("zh-Hans", "file_dialog.supported_files"),
            "支持的文件 (*.pdf *.jpg *.jpeg *.png *.tif *.tiff *.bmp)",
        )
        self.assertEqual(translate("en", "settings.font_engine.gdi"), "GDI (disable DirectWrite)")
        self.assertEqual(translate("zh-Hans", "settings.font_engine.gdi"), "GDI（禁用 DirectWrite）")

    def test_queue_worker_status_localizes_canonical_status(self) -> None:
        window = MainWindow.__new__(MainWindow)

        window.app_settings = {"language": "zh-Hans"}
        self.assertEqual(window._worker_status_text("Queue 3/3"), translate("zh-Hans", "status.queue_detail", detail="3/3"))
        self.assertEqual(window._worker_status_text("Queue 3/3, pausing send"), translate("zh-Hans", "status.queue_detail", detail="3/3"))

        window.app_settings = {"language": "en"}
        translated_queue = translate("zh-Hans", "status.queue_detail", detail="3/3")
        self.assertEqual(window._worker_status_text(translated_queue), translated_queue)

    def test_skip_reason_uses_translation_key_without_reparsing_text(self) -> None:
        window = MainWindow.__new__(MainWindow)
        skipped = SkippedTaskInput(Path("missing.pdf"), "File does not exist", "task.skip.missing_file")

        window.app_settings = {"language": "zh-Hans"}
        self.assertEqual(window._task_skip_reason_text(skipped), translate("zh-Hans", "task.skip.missing_file"))

        window.app_settings = {"language": "en"}
        self.assertEqual(window._task_skip_reason_text(skipped), "File does not exist")
        self.assertEqual(window._task_skip_reason_text(SkippedTaskInput(Path("bad.pdf"), "broken pdf")), "broken pdf")
