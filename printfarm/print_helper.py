from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
import argparse
import json
import subprocess
import sys
import threading

from .debug_logger import debug_exception, debug_log, set_debug_forwarder
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
#                      {"cmd": "release"}
#                      {"cmd": "exit"}
#   helper -> parent : {"event": "ready"}                    after startup
#                      {"event": "fatal", "message"}         startup failed
#                      {"event": "job_started", "job_id"}    right after StartDoc
#                      {"event": "done"}                     copy submitted
#                      {"event": "released"}                 prepared DIBs freed
#                      {"event": "error", "message"}         copy failed
#                      {"event": "debug", "message"}         debug line to relay
#
# The helper has no debug file of its own, so its debug_log() lines (draw
# rects, predecode decisions, exception tracebacks from the submit path) are
# forwarded as "debug" events and written into the parent's debug.log, keeping
# them in the run's timeline. Helper stderr is drained to debug.log as well so
# a hard crash still leaves a trace.


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

    def __init__(self, language: str = "en", log_callback: Callable[[str], None] | None = None) -> None:
        self.language = normalize_language(language)
        self._log_callback = log_callback
        self._proc: subprocess.Popen[str] | None = None
        self._lock = threading.Lock()
        self._terminated = False
        # Transient counterpart of _terminated for kill_inflight(): makes the
        # interrupted submit surface as "force-stopped" rather than "crashed",
        # but does not latch the client shut. Cleared once consumed or when a
        # fresh helper is started.
        self._kill_pending = False
        # (printer_name, job_id) of a submit currently in flight. Cleared when
        # the helper confirms done/error; if the helper dies first, the record
        # is used to delete the half-submitted spooler job (see _exit_error).
        self._inflight_job: tuple[str, int] | None = None

    def _t(self, key: str, **kwargs: object) -> str:
        return translate(self.language, key, **kwargs)

    def _log(self, message: str) -> None:
        if self._log_callback is None:
            return
        try:
            self._log_callback(message)
        except Exception as exc:
            debug_exception("PrintHelperClient._log", exc)

    def ensure_started(self) -> None:
        with self._lock:
            if self._terminated:
                raise RuntimeError(self._t("print_helper.terminated"))
            if self._proc is not None and self._proc.poll() is None:
                return
            self._kill_pending = False
            command = _print_helper_command(self.language)
            debug_log(f"print helper starting: {command}")
            creationflags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
            try:
                self._proc = subprocess.Popen(
                    command,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                    creationflags=creationflags,
                )
            except Exception as exc:
                raise RuntimeError(self._t("print_helper.start_failed", error=exc)) from exc
            proc = self._proc
            threading.Thread(
                target=self._drain_stderr,
                args=(proc,),
                daemon=True,
                name=f"PrintHelperStderr-{proc.pid}",
            ).start()
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
        while True:
            event = self._read_event(proc)
            kind = event.get("event")
            if kind == "job_started":
                with self._lock:
                    self._inflight_job = (printer_name, int(event.get("job_id", 0)))
                continue
            # Any helper-confirmed outcome means the helper itself settled the
            # job (EndDoc on done, AbortDoc on error): nothing left to clean.
            with self._lock:
                self._inflight_job = None
            if kind == "done":
                return
            if kind == "error":
                raise RuntimeError(str(event.get("message", "")))
            raise RuntimeError(self._t("print_helper.protocol_error", line=str(event)))

    def _read_event(self, proc: subprocess.Popen[str]) -> dict[str, Any]:
        assert proc.stdout is not None
        while True:
            line = proc.stdout.readline()
            if not line:
                raise self._exit_error(proc)
            try:
                data = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(self._t("print_helper.protocol_error", line=line.strip())) from exc
            if not isinstance(data, dict):
                raise RuntimeError(self._t("print_helper.protocol_error", line=line.strip()))
            # Relayed helper debug lines land in the parent's debug.log so the
            # submit-path diagnostics stay in the run's timeline; they are not
            # protocol answers, so keep reading.
            if data.get("event") == "debug":
                debug_log(f"print helper[{proc.pid}] {data.get('message', '')}")
                continue
            return data

    def release_pages(self) -> None:
        """Best-effort release of DIBs cached for the just-finished batch."""
        with self._lock:
            proc = self._proc
        if proc is None or proc.poll() is not None:
            return
        try:
            assert proc.stdin is not None
            proc.stdin.write('{"cmd": "release"}\n')
            proc.stdin.flush()
            event = self._read_event(proc)
            if event.get("event") != "released":
                raise RuntimeError(self._t("print_helper.protocol_error", line=str(event)))
        except Exception as exc:
            # A dead helper has already released its process memory. Cache
            # cleanup must never turn successfully submitted copies into a
            # failed batch, so diagnostics are sufficient here.
            debug_exception("PrintHelperClient.release_pages", exc)

    @staticmethod
    def _drain_stderr(proc: subprocess.Popen[str]) -> None:
        """Relay helper stderr into debug.log (crash tracebacks would otherwise
        vanish; the pipe must be drained anyway so it can never fill and block
        the helper)."""
        stream = proc.stderr
        if stream is None:
            return
        try:
            for line in stream:
                text = line.rstrip()
                if text:
                    debug_log(f"print helper[{proc.pid}] stderr: {text}")
        except Exception as exc:
            debug_exception("PrintHelperClient._drain_stderr", exc)

    def _exit_error(self, proc: subprocess.Popen[str]) -> RuntimeError:
        """Build the error for a dead helper, cleaning up its interrupted job first.

        Runs on the worker thread that was blocked on the pipe, right after the
        helper died (force stop kill or crash), so the half-submitted spooler
        job can be deleted before the batch fails through the error path.
        """
        self._cleanup_interrupted_job()
        exit_code = proc.poll()
        with self._lock:
            terminated = self._terminated or self._kill_pending
            self._kill_pending = False
        if terminated:
            return RuntimeError(self._t("print_helper.terminated"))
        return RuntimeError(self._t("print_helper.crashed", code=exit_code))

    def _cleanup_interrupted_job(self) -> None:
        with self._lock:
            job = self._inflight_job
            self._inflight_job = None
        if job is None:
            return
        printer_name, job_id = job
        if job_id <= 0:
            return
        try:
            from .spooler import delete_spooler_job

            delete_spooler_job(printer_name, job_id)
        except Exception as exc:
            debug_exception("PrintHelperClient._cleanup_interrupted_job", exc)
            self._log(self._t("print_helper.inflight_job_delete_failed", printer_name=printer_name, job_id=job_id, error=exc))
            return
        debug_log(f"print helper interrupted job deleted printer={printer_name} job_id={job_id}")
        self._log(self._t("print_helper.inflight_job_deleted", printer_name=printer_name, job_id=job_id))

    def terminate(self) -> None:
        """Hard-kill the helper (force stop). Safe to call from any thread.

        Latches the client shut: no further submissions are possible. For a
        kill that must leave the run alive, use kill_inflight() instead.
        """
        with self._lock:
            self._terminated = True
            proc = self._proc
        if proc is not None and proc.poll() is None:
            debug_log(f"print helper terminating pid={proc.pid}")
            try:
                proc.kill()
            except Exception as exc:
                debug_exception("PrintHelperClient.terminate", exc)

    def kill_inflight(self) -> None:
        """Hard-kill the helper to interrupt the current submission only.

        Print-queue maintenance path: the interrupted copy fails through the
        normal error path (including deletion of the half-submitted spooler
        job), but the client stays usable — the next print_copy() starts a
        fresh helper. Safe to call from any thread; a no-op when no helper
        process is alive.
        """
        with self._lock:
            proc = self._proc
            if proc is None or proc.poll() is not None:
                return
            self._kill_pending = True
        debug_log(f"print helper killing in-flight submission pid={proc.pid}")
        try:
            proc.kill()
        except Exception as exc:
            debug_exception("PrintHelperClient.kill_inflight", exc)
            return
        try:
            # The pipe EOF unblocks the worker before the process handle is
            # signaled (~16 ms window); wait for the real exit so a submission
            # attempted right after this cannot reuse the dying process via
            # ensure_started().
            proc.wait(timeout=5)
        except Exception as exc:
            debug_exception("PrintHelperClient.kill_inflight", exc)

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
    # The parent side of the pipe is explicitly UTF-8.  Windows otherwise
    # gives these redirected child streams the active ANSI code page, which
    # corrupts non-ASCII protocol fields in both directions.
    for stream in (sys.stdin, sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="InkSwarm print submission helper")
    parser.add_argument("--language", default="en")
    args = parser.parse_args(argv)
    language = normalize_language(args.language)

    out = sys.stdout

    def emit(payload: dict[str, Any]) -> None:
        out.write(json.dumps(payload, ensure_ascii=False) + "\n")
        out.flush()

    # This process has no debug file; ship debug_log() lines (draw rects,
    # predecode decisions, submit-path exceptions) to the parent instead,
    # which writes them into the main debug.log.
    set_debug_forwarder(lambda message: emit({"event": "debug", "message": message}))

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
        if command.get("cmd") == "release":
            prepared = None
            prepared_key = None
            emit({"event": "released"})
            continue
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
