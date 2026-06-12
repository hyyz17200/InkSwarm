from __future__ import annotations

import builtins
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from printfarm import printui
from printfarm.run_service import RunService


class FakeDevMode:
    def __init__(self, driver_extra: int = 0) -> None:
        self.Size = 220
        self.DriverData = b"x" * max(0, driver_extra)


class PrintUIDefaultsTests(TestCase):
    def _install_fake_modules(self, fake_print: SimpleNamespace):
        fake_con = SimpleNamespace(DM_OUT_BUFFER=2, DM_IN_BUFFER=8, IDOK=1)
        fake_types = SimpleNamespace(DEVMODEType=FakeDevMode)
        return patch.dict(
            sys.modules,
            {
                "win32print": fake_print,
                "win32con": fake_con,
                "pywintypes": fake_types,
            },
        )

    def test_initialized_defaults_are_left_unchanged(self) -> None:
        set_calls: list[tuple] = []
        handles = [object()]

        def open_printer(name, defaults=None):
            return handles[0]

        def get_printer(handle, level):
            self.assertEqual(level, 8)
            return {"pDevMode": FakeDevMode(12)}

        fake_print = SimpleNamespace(
            PRINTER_ACCESS_ADMINISTER=4,
            PRINTER_ACCESS_USE=8,
            OpenPrinter=open_printer,
            ClosePrinter=lambda handle: None,
            GetPrinter=get_printer,
            DocumentProperties=lambda *args: self.fail("DocumentProperties should not be called"),
            SetPrinter=lambda *args: set_calls.append(args),
        )

        with patch("printfarm.printui.sys.platform", "win32"), self._install_fake_modules(fake_print):
            self.assertFalse(printui.ensure_printer_defaults_initialized("PrinterA"))

        self.assertEqual(set_calls, [])

    def test_missing_defaults_are_created_from_driver_devmode(self) -> None:
        set_calls: list[tuple] = []
        open_defaults: list[dict | None] = []

        def open_printer(name, defaults=None):
            open_defaults.append(defaults)
            return object()

        def get_printer(handle, level):
            return {"pDevMode": None}

        def document_properties(hwnd, handle, name, output, input_devmode, mode):
            if mode == 0:
                return 232
            if output is not None and input_devmode is None and mode == 2:
                self.assertEqual(len(output.DriverData), 12)
                return 1
            if output is input_devmode and mode == 10:
                return 1
            self.fail(f"unexpected DocumentProperties mode={mode}")

        def set_printer(handle, level, payload, command):
            set_calls.append((level, payload, command))

        fake_print = SimpleNamespace(
            PRINTER_ACCESS_ADMINISTER=4,
            PRINTER_ACCESS_USE=8,
            OpenPrinter=open_printer,
            ClosePrinter=lambda handle: None,
            GetPrinter=get_printer,
            DocumentProperties=document_properties,
            SetPrinter=set_printer,
        )

        with patch("printfarm.printui.sys.platform", "win32"), self._install_fake_modules(fake_print):
            self.assertTrue(printui.ensure_printer_defaults_initialized("PrinterA"))

        self.assertEqual(open_defaults, [None, {"DesiredAccess": 12}])
        self.assertEqual(len(set_calls), 1)
        level, payload, command = set_calls[0]
        self.assertEqual(level, 8)
        self.assertEqual(command, 0)
        self.assertEqual(len(payload["pDevMode"].DriverData), 12)

    def test_save_can_skip_default_initialization_check(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "settings.dat"
            with (
                patch("printfarm.printui.sys.platform", "win32"),
                patch("printfarm.printui.ensure_printer_defaults_initialized") as ensure_defaults,
                patch("printfarm.printui._run_printui") as run_printui,
            ):
                printui.save_printer_settings("PrinterA", target, initialize_defaults=False)

        ensure_defaults.assert_not_called()
        run_printui.assert_called_once()

    def test_windows_only_message_defaults_to_english_and_can_localize(self) -> None:
        with patch("printfarm.printui.sys.platform", "linux"):
            with self.assertRaisesRegex(RuntimeError, "PrintUI is only supported on Windows"):
                printui.open_printer_preferences("PrinterA")
            with self.assertRaisesRegex(RuntimeError, "PrintUI 仅支持 Windows"):
                printui.open_printer_preferences("PrinterA", language="zh-Hans")

    def test_dependency_errors_can_localize_to_chinese(self) -> None:
        real_import = builtins.__import__

        def missing_import(name, *args, **kwargs):
            if name == "pywintypes":
                raise ModuleNotFoundError("No module named 'pywintypes'", name="pywintypes")
            return real_import(name, *args, **kwargs)

        with (
            patch("printfarm.printui.sys.platform", "win32"),
            patch("builtins.__import__", side_effect=missing_import),
        ):
            with self.assertRaisesRegex(RuntimeError, "缺少 Windows 打印依赖: pywintypes"):
                printui.ensure_printer_defaults_initialized("PrinterA", language="zh-Hans")

        def broken_import(name, *args, **kwargs):
            if name == "win32con":
                raise ImportError("bad win32con")
            return real_import(name, *args, **kwargs)

        with (
            patch("printfarm.printui.sys.platform", "win32"),
            patch.dict(sys.modules, {"pywintypes": SimpleNamespace(), "win32print": SimpleNamespace()}),
            patch("builtins.__import__", side_effect=broken_import),
        ):
            with self.assertRaisesRegex(RuntimeError, "Windows 打印依赖导入失败: bad win32con"):
                printui.ensure_printer_defaults_initialized("PrinterA", language="zh-Hans")

    def test_driver_default_devmode_error_can_localize_to_chinese(self) -> None:
        fake_print = SimpleNamespace(DocumentProperties=lambda *args: 0)
        fake_con = SimpleNamespace(DM_OUT_BUFFER=2, IDOK=1)
        fake_types = SimpleNamespace(DEVMODEType=FakeDevMode)

        with self.assertRaisesRegex(RuntimeError, "DocumentProperties 返回 0"):
            printui._driver_default_devmode(
                fake_print,
                fake_con,
                fake_types,
                object(),
                "PrinterA",
                language="zh-Hans",
            )

    def test_incomplete_devmode_error_can_localize_to_chinese(self) -> None:
        fake_print = SimpleNamespace(
            OpenPrinter=lambda *args, **kwargs: object(),
            ClosePrinter=lambda handle: None,
            GetPrinter=lambda handle, level: {"pDevMode": None},
            DocumentProperties=lambda *args: 1,
        )

        with patch("printfarm.printui.sys.platform", "win32"), self._install_fake_modules(fake_print):
            with self.assertRaisesRegex(RuntimeError, "无法读取完整的打印机默认 DEVMODE"):
                printui.ensure_printer_defaults_initialized("PrinterA", language="zh-Hans")


class RunServicePrinterDefaultsOptionTests(TestCase):
    def test_printer_defaults_check_defaults_to_enabled(self) -> None:
        options = RunService.build_run_options({})

        self.assertTrue(options.printer_defaults_check_enabled)

    def test_printer_defaults_check_can_be_disabled(self) -> None:
        options = RunService.build_run_options({"printer_defaults_check_enabled": False})

        self.assertFalse(options.printer_defaults_check_enabled)
