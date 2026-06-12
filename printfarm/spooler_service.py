from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import ctypes
from ctypes import wintypes
import json
import os
import subprocess
import sys
import tempfile
import time
from typing import Callable
import uuid

from .i18n import normalize_language, translate


SC_MANAGER_CONNECT = 0x0001
SC_STATUS_PROCESS_INFO = 0

SERVICE_QUERY_STATUS = 0x0004
SERVICE_START = 0x0010
SERVICE_STOP = 0x0020

SERVICE_CONTROL_STOP = 0x00000001

SERVICE_STOPPED = 0x00000001
SERVICE_START_PENDING = 0x00000002
SERVICE_STOP_PENDING = 0x00000003
SERVICE_RUNNING = 0x00000004

ERROR_SERVICE_NOT_ACTIVE = 1062
ERROR_SERVICE_ALREADY_RUNNING = 1056
ERROR_CANCELLED = 1223

SEE_MASK_NOCLOSEPROCESS = 0x00000040
SW_SHOWNORMAL = 1
WAIT_OBJECT_0 = 0x00000000
WAIT_TIMEOUT = 0x00000102


class SpoolerMaintenanceError(RuntimeError):
    pass


class SpoolerMaintenanceCancelled(SpoolerMaintenanceError):
    pass


@dataclass
class SpoolerMaintenanceResult:
    steps: list[str]


@dataclass(frozen=True)
class SpoolerServiceStatus:
    state: int
    process_id: int
    checkpoint: int
    wait_hint_ms: int


class SERVICE_STATUS(ctypes.Structure):
    _fields_ = [
        ("dwServiceType", wintypes.DWORD),
        ("dwCurrentState", wintypes.DWORD),
        ("dwControlsAccepted", wintypes.DWORD),
        ("dwWin32ExitCode", wintypes.DWORD),
        ("dwServiceSpecificExitCode", wintypes.DWORD),
        ("dwCheckPoint", wintypes.DWORD),
        ("dwWaitHint", wintypes.DWORD),
    ]


class SERVICE_STATUS_PROCESS(ctypes.Structure):
    _fields_ = [
        ("dwServiceType", wintypes.DWORD),
        ("dwCurrentState", wintypes.DWORD),
        ("dwControlsAccepted", wintypes.DWORD),
        ("dwWin32ExitCode", wintypes.DWORD),
        ("dwServiceSpecificExitCode", wintypes.DWORD),
        ("dwCheckPoint", wintypes.DWORD),
        ("dwWaitHint", wintypes.DWORD),
        ("dwProcessId", wintypes.DWORD),
        ("dwServiceFlags", wintypes.DWORD),
    ]


class SHELLEXECUTEINFO(ctypes.Structure):
    _fields_ = [
        ("cbSize", wintypes.DWORD),
        ("fMask", wintypes.ULONG),
        ("hwnd", wintypes.HWND),
        ("lpVerb", wintypes.LPCWSTR),
        ("lpFile", wintypes.LPCWSTR),
        ("lpParameters", wintypes.LPCWSTR),
        ("lpDirectory", wintypes.LPCWSTR),
        ("nShow", ctypes.c_int),
        ("hInstApp", wintypes.HINSTANCE),
        ("lpIDList", ctypes.c_void_p),
        ("lpClass", wintypes.LPCWSTR),
        ("hkeyClass", wintypes.HKEY),
        ("dwHotKey", wintypes.DWORD),
        ("hIcon", wintypes.HANDLE),
        ("hProcess", wintypes.HANDLE),
    ]


class SpoolerMaintenance:
    def __init__(
        self,
        service_name: str = "Spooler",
        timeout_seconds: float = 120.0,
        language: str = "en",
    ) -> None:
        self.language = normalize_language(language)
        if sys.platform != "win32":
            raise SpoolerMaintenanceError(self._t("spooler_maintenance.windows_only"))
        self.service_name = service_name
        self.timeout_seconds = timeout_seconds
        self._advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
        self._configure_api()

    def _t(self, key: str, **kwargs: object) -> str:
        return translate(self.language, key, **kwargs)

    @staticmethod
    def is_process_elevated() -> bool:
        if sys.platform != "win32":
            return False
        try:
            return bool(ctypes.windll.shell32.IsUserAnAdmin())
        except Exception:
            return False

    def restart(self, log: Callable[[str], None] | None = None) -> SpoolerMaintenanceResult:
        if not self.is_process_elevated():
            raise SpoolerMaintenanceError(self._t("spooler_maintenance.admin_required"))

        steps: list[str] = []

        def record(message: str) -> None:
            steps.append(message)
            if log is not None:
                log(message)

        before_status = self.query_status()
        record(self._t("spooler_maintenance.current_status", status=self._format_status(before_status)))
        record(self._t("spooler_maintenance.prepare_stop"))
        stopped_status = self.stop(log=record)

        record(self._t("spooler_maintenance.prepare_start"))
        started_status = self.start(log=record)
        if stopped_status.process_id:
            raise SpoolerMaintenanceError(
                self._t("spooler_maintenance.stop_pid_still_present", pid=stopped_status.process_id)
            )
        if before_status.process_id and started_status.process_id == before_status.process_id:
            raise SpoolerMaintenanceError(
                self._t("spooler_maintenance.pid_unchanged", pid=started_status.process_id)
            )
        return SpoolerMaintenanceResult(steps=steps)

    def stop(self, log: Callable[[str], None] | None = None) -> SpoolerServiceStatus:
        with self._open_service(SERVICE_QUERY_STATUS | SERVICE_STOP) as service:
            status = self._query_status(service)
            if status.state == SERVICE_STOPPED:
                self._log(log, self._t("spooler_maintenance.already_stopped"))
                return status
            self._log(log, self._t("spooler_maintenance.before_stop_status", status=self._format_status(status)))
            if not self._advapi32.ControlService(service, SERVICE_CONTROL_STOP, ctypes.byref(SERVICE_STATUS())):
                error = ctypes.get_last_error()
                if error != ERROR_SERVICE_NOT_ACTIVE:
                    self._raise_last_error(self._t("spooler_maintenance.action.stop"))
            return self._wait_for_state(service, SERVICE_STOPPED, self._t("spooler_maintenance.action.stop"), log)

    def start(self, log: Callable[[str], None] | None = None) -> SpoolerServiceStatus:
        with self._open_service(SERVICE_QUERY_STATUS | SERVICE_START) as service:
            status = self._query_status(service)
            if status.state == SERVICE_RUNNING:
                self._log(log, self._t("spooler_maintenance.already_running"))
                return status
            if not self._advapi32.StartServiceW(service, 0, None):
                error = ctypes.get_last_error()
                if error != ERROR_SERVICE_ALREADY_RUNNING:
                    self._raise_last_error(self._t("spooler_maintenance.action.start"))
            return self._wait_for_state(service, SERVICE_RUNNING, self._t("spooler_maintenance.action.start"), log)

    def query_state(self) -> int:
        return self.query_status().state

    def query_status(self) -> SpoolerServiceStatus:
        with self._open_service(SERVICE_QUERY_STATUS) as service:
            return self._query_status(service)

    def _configure_api(self) -> None:
        self._advapi32.OpenSCManagerW.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
        self._advapi32.OpenSCManagerW.restype = wintypes.HANDLE
        self._advapi32.OpenServiceW.argtypes = [wintypes.HANDLE, wintypes.LPCWSTR, wintypes.DWORD]
        self._advapi32.OpenServiceW.restype = wintypes.HANDLE
        self._advapi32.CloseServiceHandle.argtypes = [wintypes.HANDLE]
        self._advapi32.CloseServiceHandle.restype = wintypes.BOOL
        self._advapi32.QueryServiceStatusEx.argtypes = [
            wintypes.HANDLE,
            wintypes.DWORD,
            ctypes.c_void_p,
            wintypes.DWORD,
            ctypes.POINTER(wintypes.DWORD),
        ]
        self._advapi32.QueryServiceStatusEx.restype = wintypes.BOOL
        self._advapi32.ControlService.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(SERVICE_STATUS)]
        self._advapi32.ControlService.restype = wintypes.BOOL
        self._advapi32.StartServiceW.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.c_void_p]
        self._advapi32.StartServiceW.restype = wintypes.BOOL

    def _open_service(self, access: int):
        maintenance = self

        class _ServiceHandle:
            def __enter__(self):
                self.scm = maintenance._advapi32.OpenSCManagerW(None, None, SC_MANAGER_CONNECT)
                if not self.scm:
                    maintenance._raise_last_error(maintenance._t("spooler_maintenance.open_scm"))
                self.service = maintenance._advapi32.OpenServiceW(self.scm, maintenance.service_name, access)
                if not self.service:
                    maintenance._advapi32.CloseServiceHandle(self.scm)
                    maintenance._raise_last_error(maintenance._t("spooler_maintenance.open_service"))
                return self.service

            def __exit__(self, exc_type, exc, tb):
                if getattr(self, "service", None):
                    maintenance._advapi32.CloseServiceHandle(self.service)
                if getattr(self, "scm", None):
                    maintenance._advapi32.CloseServiceHandle(self.scm)
                return False

        return _ServiceHandle()

    def _query_state(self, service) -> int:
        return self._query_status(service).state

    def _query_status(self, service) -> SpoolerServiceStatus:
        status = SERVICE_STATUS_PROCESS()
        needed = wintypes.DWORD()
        if not self._advapi32.QueryServiceStatusEx(
            service,
            SC_STATUS_PROCESS_INFO,
            ctypes.byref(status),
            ctypes.sizeof(status),
            ctypes.byref(needed),
        ):
            self._raise_last_error(self._t("spooler_maintenance.query_status"))
        return SpoolerServiceStatus(
            state=int(status.dwCurrentState),
            process_id=int(status.dwProcessId),
            checkpoint=int(status.dwCheckPoint),
            wait_hint_ms=int(status.dwWaitHint),
        )

    def _wait_for_state(self, service, target_state: int, action_label: str, log: Callable[[str], None] | None = None) -> SpoolerServiceStatus:
        deadline = time.monotonic() + max(1.0, self.timeout_seconds)
        last_log_time = 0.0
        last_status_text = ""
        while True:
            status = self._query_status(service)
            status_text = self._format_status(status)
            if status.state == target_state:
                self._log(log, self._t("spooler_maintenance.confirmed", action=action_label, status=status_text))
                return status
            now = time.monotonic()
            if status_text != last_status_text or now - last_log_time >= 5.0:
                self._log(log, self._t("spooler_maintenance.waiting", action=action_label, status=status_text))
                last_status_text = status_text
                last_log_time = now
            if now >= deadline:
                raise SpoolerMaintenanceError(
                    self._t("spooler_maintenance.timeout", action=action_label, status=status_text)
                )
            time.sleep(0.25)

    @staticmethod
    def _log(log: Callable[[str], None] | None, message: str) -> None:
        if log is not None:
            log(message)

    def _raise_last_error(self, action: str) -> None:
        error = ctypes.get_last_error()
        message = ctypes.FormatError(error).strip()
        raise SpoolerMaintenanceError(
            self._t("spooler_maintenance.action_failed", action=action, message=message, error=error)
        )

    def _format_status(self, status: SpoolerServiceStatus) -> str:
        names = {
            SERVICE_STOPPED: "STOPPED",
            SERVICE_START_PENDING: "START_PENDING",
            SERVICE_STOP_PENDING: "STOP_PENDING",
            SERVICE_RUNNING: "RUNNING",
        }
        name = names.get(status.state, self._t("spooler_maintenance.status_code", state=status.state))
        return (
            f"{name} / PID {status.process_id} / "
            f"checkpoint {status.checkpoint} / wait_hint {status.wait_hint_ms}ms"
        )


def run_elevated_spooler_maintenance(
    timeout_seconds: float = 120.0,
    log: Callable[[str], None] | None = None,
    language: str = "en",
) -> SpoolerMaintenanceResult:
    language = normalize_language(language)
    if sys.platform != "win32":
        raise SpoolerMaintenanceError(translate(language, "spooler_maintenance.windows_only"))

    token = uuid.uuid4().hex
    temp_dir = Path(tempfile.gettempdir())
    result_file = temp_dir / f"inkswarm-spooler-maintenance-{token}.json"
    events_file = temp_dir / f"inkswarm-spooler-maintenance-{token}.events.jsonl"
    file_path, parameters, working_dir = _elevated_helper_command(result_file, events_file, timeout_seconds, language)
    process_handle = _shell_execute_runas(file_path, parameters, working_dir, language)

    try:
        exit_code = _wait_for_helper(process_handle, events_file, timeout_seconds + 90.0, log, language)
    finally:
        ctypes.windll.kernel32.CloseHandle(process_handle)

    if not result_file.exists():
        raise SpoolerMaintenanceError(
            translate(language, "spooler_maintenance.helper_missing_result", exit_code=exit_code)
        )

    try:
        data = json.loads(result_file.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SpoolerMaintenanceError(
            translate(language, "spooler_maintenance.helper_result_read_failed", error=exc)
        ) from exc
    finally:
        _cleanup_temp_file(result_file)
        _cleanup_temp_file(events_file)

    steps = [str(item) for item in data.get("steps", [])]
    if not data.get("ok"):
        error = str(data.get("error") or translate(language, "spooler_maintenance.helper_failed"))
        raise SpoolerMaintenanceError(error)
    return SpoolerMaintenanceResult(steps=steps)


def run_spooler_maintenance_helper(
    result_file: Path,
    events_file: Path,
    timeout_seconds: float,
    language: str = "en",
) -> int:
    language = normalize_language(language)
    steps: list[str] = []

    def record(message: str) -> None:
        steps.append(message)
        _append_event(events_file, message)

    payload: dict[str, object]
    try:
        result = SpoolerMaintenance(timeout_seconds=timeout_seconds, language=language).restart(log=record)
        payload = {
            "ok": True,
            "steps": steps,
        }
        exit_code = 0
    except Exception as exc:
        payload = {
            "ok": False,
            "steps": steps,
            "error": str(exc),
        }
        exit_code = 2

    _write_json_atomic(result_file, payload)
    return exit_code


def _elevated_helper_command(
    result_file: Path,
    events_file: Path,
    timeout_seconds: float,
    language: str = "en",
) -> tuple[str, str, str]:
    language = normalize_language(language)
    app_path = _current_app_path()
    helper_args = [
        "--spooler-helper",
        "--result-file",
        str(result_file),
        "--events-file",
        str(events_file),
        "--timeout",
        str(float(timeout_seconds)),
        "--language",
        language,
    ]
    if app_path.suffix.lower() in {".py", ".pyw"}:
        file_path = sys.executable
        args = [str(app_path), *helper_args]
    else:
        file_path = str(app_path)
        args = helper_args
    return file_path, subprocess.list2cmdline(args), str(app_path.parent)


def _current_app_path() -> Path:
    argv0 = Path(sys.argv[0]).resolve()
    if argv0.exists() and argv0.suffix.lower() in {".py", ".pyw", ".exe"}:
        return argv0
    source_app = Path(__file__).resolve().parent.parent / "app.py"
    if source_app.exists():
        return source_app
    return Path(sys.executable).resolve()


def _shell_execute_runas(file_path: str, parameters: str, working_dir: str, language: str = "en") -> int:
    language = normalize_language(language)
    shell32 = ctypes.WinDLL("shell32", use_last_error=True)
    shell32.ShellExecuteExW.argtypes = [ctypes.POINTER(SHELLEXECUTEINFO)]
    shell32.ShellExecuteExW.restype = wintypes.BOOL

    info = SHELLEXECUTEINFO()
    info.cbSize = ctypes.sizeof(SHELLEXECUTEINFO)
    info.fMask = SEE_MASK_NOCLOSEPROCESS
    info.lpVerb = "runas"
    info.lpFile = file_path
    info.lpParameters = parameters
    info.lpDirectory = working_dir
    info.nShow = SW_SHOWNORMAL

    if not shell32.ShellExecuteExW(ctypes.byref(info)):
        error = ctypes.get_last_error()
        if error == ERROR_CANCELLED:
            raise SpoolerMaintenanceCancelled(translate(language, "spooler_maintenance.admin_cancelled"))
        message = ctypes.FormatError(error).strip()
        raise SpoolerMaintenanceError(
            translate(language, "spooler_maintenance.start_elevated_failed", message=message, error=error)
        )
    return int(info.hProcess)


def _wait_for_helper(
    process_handle: int,
    events_file: Path,
    timeout_seconds: float,
    log: Callable[[str], None] | None,
    language: str = "en",
) -> int:
    language = normalize_language(language)
    kernel32 = ctypes.windll.kernel32
    deadline = time.monotonic() + max(1.0, timeout_seconds)
    offset = 0
    while True:
        offset = _drain_event_file(events_file, offset, log)
        wait_ms = 250
        result = kernel32.WaitForSingleObject(process_handle, wait_ms)
        if result == WAIT_OBJECT_0:
            offset = _drain_event_file(events_file, offset, log)
            exit_code = wintypes.DWORD()
            if not kernel32.GetExitCodeProcess(process_handle, ctypes.byref(exit_code)):
                return -1
            return int(exit_code.value)
        if result != WAIT_TIMEOUT:
            raise SpoolerMaintenanceError(
                translate(language, "spooler_maintenance.wait_helper_failed", result=result)
            )
        if time.monotonic() >= deadline:
            raise SpoolerMaintenanceError(translate(language, "spooler_maintenance.helper_timeout"))


def _drain_event_file(events_file: Path, offset: int, log: Callable[[str], None] | None) -> int:
    if not events_file.exists():
        return offset
    try:
        with events_file.open("r", encoding="utf-8") as fh:
            fh.seek(offset)
            for line in fh:
                try:
                    data = json.loads(line)
                    message = str(data.get("message") or "")
                except Exception:
                    message = line.strip()
                if message and log is not None:
                    log(message)
            return fh.tell()
    except Exception:
        return offset


def _append_event(events_file: Path, message: str) -> None:
    try:
        events_file.parent.mkdir(parents=True, exist_ok=True)
        with events_file.open("a", encoding="utf-8", buffering=1) as fh:
            fh.write(json.dumps({"message": message}, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp_path, path)


def _cleanup_temp_file(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass
