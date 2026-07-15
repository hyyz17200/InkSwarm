from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
import json

from printfarm.config_store import ConfigStore


def _write_worker(worker_dir: Path, printer_name: str = "PrinterA") -> None:
    preset_dir = worker_dir / "presets"
    preset_dir.mkdir(parents=True, exist_ok=True)
    (worker_dir / "worker.json").write_text(
        json.dumps(
            {
                "name": worker_dir.name,
                "printer_name": printer_name,
                "enabled": True,
                "weight": 1,
                "active_preset": "default",
            }
        ),
        encoding="utf-8",
    )
    (preset_dir / "default.json").write_text(
        json.dumps({"name": "default", "dpi": 300}),
        encoding="utf-8",
    )


class SaveWorkerPresetSafetyTests(TestCase):
    def test_save_worker_keeps_preset_files_added_after_load(self) -> None:
        # Presets are authored on disk (the GUI has no preset editor): a preset
        # json dropped into presets/ while the app is running must survive any
        # later save (worker buttons, closing the app) without a reload first.
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ConfigStore(root)
            worker_dir = store.default_group_dir() / "W1"
            _write_worker(worker_dir)
            workers = store.load_workers()
            self.assertEqual(len(workers), 1)

            late_preset = worker_dir / "presets" / "added-later.json"
            late_preset.write_text(json.dumps({"name": "added-later", "dpi": 600}), encoding="utf-8")

            store.save_worker(workers[0])

            self.assertTrue(late_preset.exists())
            reloaded = store.load_workers()[0]
            self.assertIn("added-later", reloaded.presets)

    def test_save_worker_still_writes_loaded_presets(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ConfigStore(root)
            worker_dir = store.default_group_dir() / "W1"
            _write_worker(worker_dir)
            worker = store.load_workers()[0]

            worker.presets["default"].dpi = 720
            store.save_worker(worker)

            saved = json.loads((worker_dir / "presets" / "default.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["dpi"], 720)


class DefensiveWorkerLoadingTests(TestCase):
    def test_invalid_worker_is_skipped_without_hiding_valid_workers(self) -> None:
        with TemporaryDirectory() as tmp:
            store = ConfigStore(Path(tmp))
            _write_worker(store.default_group_dir() / "Good")
            broken_file = store.default_group_dir() / "Broken" / "worker.json"
            broken_file.parent.mkdir(parents=True)
            broken_file.write_text('{"weight": "not-a-number"}', encoding="utf-8")

            workers = store.load_workers()

            self.assertEqual([worker.name for worker in workers], ["Good"])
            self.assertEqual(store.last_worker_load_errors[0][0], broken_file)
            self.assertIn("invalid literal", store.last_worker_load_errors[0][1])

    def test_invalid_preset_is_skipped_without_hiding_worker(self) -> None:
        with TemporaryDirectory() as tmp:
            store = ConfigStore(Path(tmp))
            worker_dir = store.default_group_dir() / "W1"
            _write_worker(worker_dir)
            broken_preset = worker_dir / "presets" / "broken.json"
            broken_preset.write_text('{"dpi": }', encoding="utf-8")

            workers = store.load_workers()

            self.assertEqual(len(workers), 1)
            self.assertEqual(list(workers[0].presets), ["default"])
            self.assertEqual(store.last_worker_load_errors[0][0], broken_preset)
