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
    def __init__(self, service_name: str = "Spooler", timeout_seconds: float = 120.0) -> None:
        if sys.platform != "win32":
            raise SpoolerMaintenanceError("重启打印队列仅支持 Windows。")
        self.service_name = service_name
        self.timeout_seconds = timeout_seconds
        self._advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
        self._configure_api()

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
            raise SpoolerMaintenanceError("当前进程没有管理员权限，无法停止/启动 Windows Print Spooler。请以管理员身份启动 InkSwarm 后重试。")

        steps: list[str] = []

        def record(message: str) -> None:
            steps.append(message)
            if log is not None:
                log(message)

        before_status = self.query_status()
        record(f"当前 Windows Print Spooler 状态: {self._format_status(before_status)}。")
        record("准备停止 Windows Print Spooler 服务。")
        stopped_status = self.stop(log=record)

        record("准备启动 Windows Print Spooler 服务。")
        started_status = self.start(log=record)
        if stopped_status.process_id:
            raise SpoolerMaintenanceError(f"Print Spooler 停止确认异常，停止后 PID 仍为 {stopped_status.process_id}。")
        if before_status.process_id and started_status.process_id == before_status.process_id:
            raise SpoolerMaintenanceError(
                f"Print Spooler 状态回到运行中，但进程 PID 没有变化，仍为 {started_status.process_id}，"
                "无法确认服务已经真实重启。"
            )
        return SpoolerMaintenanceResult(steps=steps)

    def stop(self, log: Callable[[str], None] | None = None) -> SpoolerServiceStatus:
        with self._open_service(SERVICE_QUERY_STATUS | SERVICE_STOP) as service:
            status = self._query_status(service)
            if status.state == SERVICE_STOPPED:
                self._log(log, "Windows Print Spooler 已经处于停止状态。")
                return status
            self._log(log, f"停止前状态: {self._format_status(status)}。")
            if not self._advapi32.ControlService(service, SERVICE_CONTROL_STOP, ctypes.byref(SERVICE_STATUS())):
                error = ctypes.get_last_error()
                if error != ERROR_SERVICE_NOT_ACTIVE:
                    self._raise_last_error("停止 Windows Print Spooler")
            return self._wait_for_state(service, SERVICE_STOPPED, "停止", log)

    def start(self, log: Callable[[str], None] | None = None) -> SpoolerServiceStatus:
        with self._open_service(SERVICE_QUERY_STATUS | SERVICE_START) as service:
            status = self._query_status(service)
            if status.state == SERVICE_RUNNING:
                self._log(log, "Windows Print Spooler 已经处于运行状态。")
                return status
            if not self._advapi32.StartServiceW(service, 0, None):
                error = ctypes.get_last_error()
                if error != ERROR_SERVICE_ALREADY_RUNNING:
                    self._raise_last_error("启动 Windows Print Spooler")
            return self._wait_for_state(service, SERVICE_RUNNING, "启动", log)

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
                    maintenance._raise_last_error("打开服务控制管理器")
                self.service = maintenance._advapi32.OpenServiceW(self.scm, maintenance.service_name, access)
                if not self.service:
                    maintenance._advapi32.CloseServiceHandle(self.scm)
                    maintenance._raise_last_error("打开 Windows Print Spooler 服务")
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
            self._raise_last_error("查询 Windows Print Spooler 状态")
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
                self._log(log, f"Windows Print Spooler 已确认{action_label}完成: {status_text}。")
                return status
            now = time.monotonic()
            if status_text != last_status_text or now - last_log_time >= 5.0:
                self._log(log, f"等待 Windows Print Spooler {action_label}中: {status_text}。")
                last_status_text = status_text
                last_log_time = now
            if now >= deadline:
                raise SpoolerMaintenanceError(f"等待 Windows Print Spooler {action_label}超时，当前状态: {status_text}")
            time.sleep(0.25)

    @staticmethod
    def _log(log: Callable[[str], None] | None, message: str) -> None:
        if log is not None:
            log(message)

    @staticmethod
    def _raise_last_error(action: str) -> None:
        error = ctypes.get_last_error()
        message = ctypes.FormatError(error).strip()
        raise SpoolerMaintenanceError(f"{action}失败: {message} (WinError {error})")

    @staticmethod
    def _format_status(status: SpoolerServiceStatus) -> str:
        names = {
            SERVICE_STOPPED: "STOPPED",
            SERVICE_START_PENDING: "START_PENDING",
            SERVICE_STOP_PENDING: "STOP_PENDING",
            SERVICE_RUNNING: "RUNNING",
        }
        name = names.get(status.state, f"状态码 {status.state}")
        return (
            f"{name} / PID {status.process_id} / "
            f"checkpoint {status.checkpoint} / wait_hint {status.wait_hint_ms}ms"
        )


def run_elevated_spooler_maintenance(
    timeout_seconds: float = 120.0,
    log: Callable[[str], None] | None = None,
) -> SpoolerMaintenanceResult:
    if sys.platform != "win32":
        raise SpoolerMaintenanceError("重启打印队列仅支持 Windows。")

    token = uuid.uuid4().hex
    temp_dir = Path(tempfile.gettempdir())
    result_file = temp_dir / f"inkswarm-spooler-maintenance-{token}.json"
    events_file = temp_dir / f"inkswarm-spooler-maintenance-{token}.events.jsonl"
    file_path, parameters, working_dir = _elevated_helper_command(result_file, events_file, timeout_seconds)
    process_handle = _shell_execute_runas(file_path, parameters, working_dir)

    try:
        exit_code = _wait_for_helper(process_handle, events_file, timeout_seconds + 90.0, log)
    finally:
        ctypes.windll.kernel32.CloseHandle(process_handle)

    if not result_file.exists():
        raise SpoolerMaintenanceError(f"管理员维护进程没有写回结果，退出码: {exit_code}")

    try:
        data = json.loads(result_file.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SpoolerMaintenanceError(f"读取管理员维护结果失败: {exc}") from exc
    finally:
        _cleanup_temp_file(result_file)
        _cleanup_temp_file(events_file)

    steps = [str(item) for item in data.get("steps", [])]
    if not data.get("ok"):
        error = str(data.get("error") or "管理员维护进程执行失败。")
        raise SpoolerMaintenanceError(error)
    return SpoolerMaintenanceResult(steps=steps)


def run_spooler_maintenance_helper(result_file: Path, events_file: Path, timeout_seconds: float) -> int:
    steps: list[str] = []

    def record(message: str) -> None:
        steps.append(message)
        _append_event(events_file, message)

    payload: dict[str, object]
    try:
        result = SpoolerMaintenance(timeout_seconds=timeout_seconds).restart(log=record)
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


def _elevated_helper_command(result_file: Path, events_file: Path, timeout_seconds: float) -> tuple[str, str, str]:
    app_path = _current_app_path()
    helper_args = [
        "--spooler-helper",
        "--result-file",
        str(result_file),
        "--events-file",
        str(events_file),
        "--timeout",
        str(float(timeout_seconds)),
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


def _shell_execute_runas(file_path: str, parameters: str, working_dir: str) -> int:
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
            raise SpoolerMaintenanceCancelled("用户取消了管理员权限授权，未执行 Print Spooler 操作。")
        message = ctypes.FormatError(error).strip()
        raise SpoolerMaintenanceError(f"启动管理员维护进程失败: {message} (WinError {error})")
    return int(info.hProcess)


def _wait_for_helper(
    process_handle: int,
    events_file: Path,
    timeout_seconds: float,
    log: Callable[[str], None] | None,
) -> int:
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
            raise SpoolerMaintenanceError(f"等待管理员维护进程失败，WaitForSingleObject={result}")
        if time.monotonic() >= deadline:
            raise SpoolerMaintenanceError("等待管理员维护进程完成超时，Print Spooler 状态可能需要手动确认。")


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
