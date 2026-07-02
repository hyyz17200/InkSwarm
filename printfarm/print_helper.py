from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
import argparse
import json
import subprocess
import sys
import threading

from .debug_logger import debug_exception, debug_log
from .i18n import normalize_language, translate

# The actual GDI submission runs in a dedicated helper subprocess, not in a
# worker thread. A GDI print call (StartDoc/StretchDIBits/EndDoc) can block
# forever inside winspool or a printer driver, and a Python thread stuck there
# can never be interrupted or killed — the only reliable "force stop" on
# Windows is terminating a process. The render cache already lives on disk, so
# the helper only needs printer name, page paths and draw options; killing it
# unblocks the owning worker thread through pipe EOF and the normal error path.
#
# Protocol (one JSON object per line):
#   parent -> helper : {"cmd": "print", "printer_name", "job_name", "page_paths",
#                       "page_specs", "ignore_margins", "fit_mode", "reuse_pages"}
#                      {"cmd": "exit"}
#   helper -> parent : {"event": "ready"}                    after startup
#                      {"event": "fatal", "message"}         startup failed
#                      {"event": "job_started", "job_id"}    right after StartDoc
#                      {"event": "done"}                     copy submitted
#                      {"event": "error", "message"}         copy failed


def _print_helper_command(language: str) -> list[str]:
    helper_args = ["--print-helper", "--language", language]
    # Running from source (including tests): relaunch app.py with the current
    # interpreter. sys.argv[0] is unreliable here (it may be pytest, an IDE
    # runner, ...), so the source tree location wins whenever it exists.
    source_app = Path(__file__).resolve().parent.parent / "app.py"
    if source_app.exists():
        return [sys.executable, str(source_app), *helper_args]
    # Frozen (Nuitka): relaunch the application binary itself.
    from .spooler_service import _current_app_path

    return [str(_current_app_path()), *helper_args]


class PrintHelperClient:
    """Parent-side handle to one print helper subprocess (one per worker).

    print_copy() is only ever called by the owning worker thread; terminate()
    may be called concurrently from the controller (force stop) and simply
    kills the process, which surfaces to the worker thread as pipe EOF.
    """

    def __init__(self, language: str = "en") -> None:
        self.language = normalize_language(language)
        self._proc: subprocess.Popen[str] | None = None
        self._lock = threading.Lock()
        self._terminated = False
        # (printer_name, job_id) of a submit currently in flight, kept for
        # post-kill cleanup of the half-submitted spooler job.
        self._inflight_job: tuple[str, int] | None = None

    def _t(self, key: str, **kwargs: object) -> str:
        return translate(self.language, key, **kwargs)

    def ensure_started(self) -> None:
        with self._lock:
            if self._terminated:
                raise RuntimeError(self._t("print_helper.terminated"))
            if self._proc is not None and self._proc.poll() is None:
                return
            command = _print_helper_command(self.language)
            debug_log(f"print helper starting: {command}")
            creationflags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
            try:
                self._proc = subprocess.Popen(
                    command,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    encoding="utf-8",
                    bufsize=1,
                    creationflags=creationflags,
                )
            except Exception as exc:
                raise RuntimeError(self._t("print_helper.start_failed", error=exc)) from exc
            proc = self._proc
        event = self._read_event(proc)
        if event.get("event") == "ready":
            debug_log(f"print helper ready pid={proc.pid}")
            return
        if event.get("event") == "fatal":
            raise RuntimeError(self._t("print_helper.not_ready", error=event.get("message", "")))
        raise RuntimeError(self._t("print_helper.protocol_error", line=str(event)))

    def print_copy(
        self,
        printer_name: str,
        job_name: str,
        page_paths: list[Path],
        page_specs: list[dict],
        ignore_margins: bool,
        fit_mode: str,
        reuse_pages: bool = False,
    ) -> None:
        self.ensure_started()
        with self._lock:
            proc = self._proc
        assert proc is not None
        payload = {
            "cmd": "print",
            "printer_name": printer_name,
            "job_name": job_name,
            "page_paths": [str(path) for path in page_paths],
            "page_specs": page_specs,
            "ignore_margins": bool(ignore_margins),
            "fit_mode": fit_mode,
            "reuse_pages": bool(reuse_pages),
        }
        try:
            assert proc.stdin is not None
            proc.stdin.write(json.dumps(payload, ensure_ascii=False) + "\n")
            proc.stdin.flush()
        except OSError as exc:
            raise self._exit_error(proc) from exc
        try:
            while True:
                event = self._read_event(proc)
                kind = event.get("event")
                if kind == "job_started":
                    with self._lock:
                        self._inflight_job = (printer_name, int(event.get("job_id", 0)))
                    continue
                if kind == "done":
                    return
                if kind == "error":
                    raise RuntimeError(str(event.get("message", "")))
                raise RuntimeError(self._t("print_helper.protocol_error", line=str(event)))
        finally:
            with self._lock:
                self._inflight_job = None

    def _read_event(self, proc: subprocess.Popen[str]) -> dict[str, Any]:
        assert proc.stdout is not None
        line = proc.stdout.readline()
        if not line:
            raise self._exit_error(proc)
        try:
            data = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(self._t("print_helper.protocol_error", line=line.strip())) from exc
        if not isinstance(data, dict):
            raise RuntimeError(self._t("print_helper.protocol_error", line=line.strip()))
        return data

    def _exit_error(self, proc: subprocess.Popen[str]) -> RuntimeError:
        exit_code = proc.poll()
        with self._lock:
            terminated = self._terminated
        if terminated:
            return RuntimeError(self._t("print_helper.terminated"))
        return RuntimeError(self._t("print_helper.crashed", code=exit_code))

    def take_inflight_job(self) -> tuple[str, int] | None:
        """Return and clear the (printer, job id) of a submit interrupted by kill."""
        with self._lock:
            job = self._inflight_job
            self._inflight_job = None
            return job

    def terminate(self) -> None:
        """Hard-kill the helper (force stop). Safe to call from any thread."""
        with self._lock:
            self._terminated = True
            proc = self._proc
        if proc is not None and proc.poll() is None:
            debug_log(f"print helper terminating pid={proc.pid}")
            try:
                proc.kill()
            except Exception as exc:
                debug_exception("PrintHelperClient.terminate", exc)

    def close(self) -> None:
        """Graceful shutdown at the end of a worker's run."""
        with self._lock:
            proc = self._proc
            self._proc = None
        if proc is None:
            return
        if proc.poll() is None:
            try:
                assert proc.stdin is not None
                proc.stdin.write('{"cmd": "exit"}\n')
                proc.stdin.flush()
            except OSError:
                pass
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
        debug_log(f"print helper closed pid={proc.pid}")


def main(argv: list[str] | None = None) -> int:
    """Entry point of the helper subprocess (app.py --print-helper)."""
    parser = argparse.ArgumentParser(description="InkSwarm print submission helper")
    parser.add_argument("--language", default="en")
    args = parser.parse_args(argv)
    language = normalize_language(args.language)

    out = sys.stdout

    def emit(payload: dict[str, Any]) -> None:
        out.write(json.dumps(payload, ensure_ascii=False) + "\n")
        out.flush()

    try:
        from .spooler import PrinterSpooler

        spooler = PrinterSpooler(language=language)
    except Exception as exc:
        emit({"event": "fatal", "message": str(exc)})
        return 2

    emit({"event": "ready"})

    # DIBs decoded for the previous multi-copy job, reused while the parent
    # keeps sending the same page set (mirrors the old per-batch predecode).
    prepared_key: tuple[str, ...] | None = None
    prepared: list[Any] | None = None

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            command = json.loads(line)
        except json.JSONDecodeError:
            emit({"event": "error", "message": f"invalid command line: {line[:200]}"})
            continue
        if not isinstance(command, dict) or command.get("cmd") == "exit":
            break
        if command.get("cmd") != "print":
            emit({"event": "error", "message": f"unknown command: {command.get('cmd')!r}"})
            continue
        try:
            page_paths = [Path(item) for item in command.get("page_paths", [])]
            page_specs = list(command.get("page_specs", []))
            reuse_pages = bool(command.get("reuse_pages"))
            if reuse_pages:
                key = tuple(str(path) for path in page_paths)
                if key != prepared_key:
                    prepared = spooler.prepare_pages(page_paths, page_specs)
                    prepared_key = key
            else:
                prepared = None
                prepared_key = None
            spooler.print_single_copy(
                printer_name=str(command.get("printer_name", "")),
                page_paths=page_paths,
                page_specs=page_specs,
                job_name=str(command.get("job_name", "")),
                ignore_margins=bool(command.get("ignore_margins", True)),
                fit_mode=str(command.get("fit_mode", "")),
                prepared=prepared,
                on_job_started=lambda job_id: emit({"event": "job_started", "job_id": job_id}),
            )
            emit({"event": "done"})
        except Exception as exc:
            # Keep serving: the worker decides whether to retry or abandon.
            emit({"event": "error", "message": str(exc)})
    return 0
