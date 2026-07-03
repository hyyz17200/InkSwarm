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
