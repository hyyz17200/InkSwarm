# Code Review Findings — dev @ `3093f93` (v0.3.15)

Full-codebase review performed ahead of the next stable release. Baseline: all 118 tests
pass on this commit. Findings 1 and 2 were **verified experimentally**, not just by reading
code. This document is written for a follow-up agent to implement fixes; each finding lists
the affected code, the failure scenario, and the recommended fix. **No code has been
modified yet.**

Priority legend:

| Priority | Meaning |
|----------|---------|
| P0 | Fix before the stable release |
| P1 | Low-probability or low-impact defect; fix is cheap and recommended |
| P2 | Improvement / hardening, at maintainer's discretion |

---

## 1. [P0] Print-helper pipe encoding mismatch (gbk ↔ utf-8) corrupts non-ASCII both ways

**Where:**
- Parent side: `printfarm/print_helper.py` — `PrintHelperClient.ensure_started()` creates the
  helper `subprocess.Popen(..., text=True, encoding="utf-8", errors="replace")` (~line 100–110).
- Child side: `printfarm/print_helper.py` — `main()` (~line 313 onward) uses raw `sys.stdin` /
  `sys.stdout` / `sys.stderr`.

**Problem:** On Chinese-locale Windows (verified on this machine: Python 3.12,
`sys.flags.utf8_mode == 0`, preferred encoding `cp936`), piped `sys.stdin`/`sys.stdout` in the
child default to **cp936**, while the parent reads/writes **utf-8**. Verified by experiment
with a simulated parent/child pair — corruption occurs in both directions:

- helper → parent: Chinese text in `error`/`debug` events arrives as U+FFFD replacement
  characters (JSON structure happens to survive, content is mojibake in `debug.log` and the
  GUI log).
- parent → helper: a `job_name` containing Chinese decodes to garbage (replacement chars and
  surrogates observed); the job name shown in the Windows print queue is mojibake.

**Worst-case impact:** `page_paths` travel over the same channel. The current dev install
path is pure ASCII, so production runs have not hit this — but if a user installs InkSwarm
into a directory containing non-ASCII characters, the helper receives corrupted cache paths
and **every print fails**. Additionally, GBK second bytes include `\` (0x5C), so specific
Chinese byte sequences can swallow the JSON path-escape backslash, breaking command parsing
entirely (copy fails, potentially helper crash).

**Fix:** At the top of `main()` in `printfarm/print_helper.py`, reconfigure all three
standard streams before any protocol I/O:

```python
sys.stdin.reconfigure(encoding="utf-8", errors="replace")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")
```

(`reconfigure` exists on `io.TextIOWrapper`; guard with `hasattr` if you want to be
defensive about frozen/Nuitka stream replacements.) Reconfiguring stderr also fixes mojibake
when the parent's `_drain_stderr` relays helper crash tracebacks into `debug.log`.

**Test suggestion:** extend `tests/test_print_helper.py` with a round-trip through a real
helper subprocess using a Chinese `job_name` and a non-ASCII page path, asserting lossless
transport.

---

## 2. [P0] One corrupt `worker.json` / preset JSON prevents the whole app from starting

**Where:**
- `printfarm/config_store.py` — `load_workers()` (~line 119: `json.loads` on `worker.json`)
  and `_load_presets_for_worker()` (~line 105: `json.loads` on each preset file). No error
  handling.
- `printfarm/gui.py` — `MainWindow.__init__` → `reload_workers()` (~line 397), no
  try/except; `run()` does not wrap `MainWindow()` construction.

**Problem (verified experimentally):** a single malformed worker or preset JSON file raises
`JSONDecodeError` all the way up. At startup the main window never appears — the app
"flash-crashes" with the only clue in `debug.log`. Clicking *Reload Workers* at runtime hits
the same unhandled path inside a Qt slot. `worker.json` is designed to be hand-edited, so
this is a realistic production scenario. Type-coercion failures in
`WorkerConfig.from_dict` (e.g. `int(data.get("weight", 1))` on a non-numeric value in
`printfarm/models.py`) are the same failure class.

**Fix:** In `load_workers()` / `_load_presets_for_worker()`, catch parse/coercion errors per
file, skip the broken worker/preset, and surface the error (return alongside results or log +
GUI notice), instead of failing the entire load. Additionally, wrap `reload_workers()`'s load
call in the GUI so any residual error shows a dialog rather than killing the window.

---

## 3. [P1] `SpoolerMaintenance.restart()` validates the stop-state PID *after* restarting

**Where:** `printfarm/spooler_service.py`, `restart()` (~lines 151–163).

**Problem:** the `if stopped_status.process_id:` check runs **after** `self.start()` has
already restarted the service. If Windows reports a lingering PID at the moment
SERVICE_STOPPED was confirmed (rare; usually 0), the method raises
`spooler_maintenance.stop_pid_still_present` even though the restart actually succeeded —
maintenance is reported as failed and the run stays paused, misleading the user.

**Fix:** move the `stopped_status.process_id` check to immediately after `self.stop(...)`
(before `self.start(...)`), or drop it entirely — the subsequent
`started_status.process_id == before_status.process_id` check already covers the
"nothing actually restarted" case.

---

## 4. [P1] Concurrent PDF rendering of *different* files is not serialized (PDFium is not thread-safe)

**Where:** `printfarm/renderer.py` — `_render_pdf()` / `_get_pdf_render_lock()`
(~lines 264–295, 329–336).

**Problem:** the render lock is keyed by PDF path, so two workers rendering two *different*
PDFs call pypdfium2 concurrently. PDFium itself is not thread-safe; pypdfium2 5.8 (pinned
`>=5.8,<6` in `requirements.txt`) does not add a global lock, and ctypes calls release the
GIL, so this is genuine native-code concurrency. Production has run for days without a
crash, but that likely reflects load patterns (one PDF at a time), not safety. Failure mode
would be a native access violation taking down the process.

**Fix:** replace the per-path lock with (or add underneath it) a single global lock around
all pypdfium2 usage in `_render_pdf()` — note `task_inspector._inspect_pdf()` also uses
pypdfium2 (GUI thread), so ideally share one module-level lock across both call sites.
Rendered output is cached per key, so the lost parallelism is limited and the determinism is
worth it.

---

## 5. [P1] EXIF 90° rotation does not swap the DPI tuple

**Where:** `printfarm/task_inspector.py` — `apply_exif_orientation()` (~line 113) combined
with `get_image_dpi()` (~line 135); consumed by `_inspect_image()` and
`printfarm/renderer.py` `_render_image_file()`.

**Problem:** `ImageOps.exif_transpose` preserves `image.info` as-is; for EXIF orientations
that rotate 90°/270° (values 5–8), the `dpi` tuple's (x, y) are not swapped. An image with
non-square DPI plus a rotating EXIF tag gets a wrong physical size (mm) — affecting
displayed size, cache key page specs, and the printed rect. Edge case: virtually all real
files have x == y DPI.

**Fix:** in `apply_exif_orientation()`, when the applied orientation is one of {5, 6, 7, 8}
(a transpose that swaps axes), swap the `dpi` (and `resolution`) tuples on the returned
image's `info` dict.

---

## 6. [P1] Printer DC leak and exception masking in `print_single_copy`

**Where:** `printfarm/spooler.py` — `print_single_copy()` (~lines 250–281).

**Problems:**
1. `dc = win32ui.CreateDC()` and `dc.CreatePrinterDC(printer_name)` run *before* the
   `try:` block — if `CreatePrinterDC` raises (bad/offline printer name), `DeleteDC` is
   never called and the DC handle leaks.
2. In the `finally:` block, a raising `dc.DeleteDC()` would mask the original print
   exception.

**Fix:** move `CreatePrinterDC` inside the `try`, and wrap the `DeleteDC()` call in its own
`try/except Exception: pass` (mirroring how `AbortDoc` is already guarded).

---

## 7. [P2] Temp-file leak on elevated-maintenance failure paths

**Where:** `printfarm/spooler_service.py` — `run_elevated_spooler_maintenance()`
(~lines 317–348).

**Problem:** `result_file` / `events_file` cleanup only happens in the `finally` attached to
the JSON-parse block. If `_wait_for_helper()` raises (timeout / wait failure) or the
`result_file` does not exist, the temp files under `%TEMP%` are never removed.

**Fix:** wrap the whole body after temp-name generation in `try/finally` that calls
`_cleanup_temp_file()` for both files (cleanup is already idempotent via
`unlink(missing_ok=True)`).

---

## 8. [P2] Predecoded DIBs stay resident in the helper after a multi-copy batch ends

**Where:** `printfarm/print_helper.py` — helper `main()` loop (`prepared_key` / `prepared`
cache, ~lines 342–371); budget `PREDECODE_MAX_BYTES = 1 GiB` in `printfarm/spooler.py`
(~line 31).

**Problem:** for multi-copy jobs the helper keeps all decoded page DIBs (up to ~1 GiB per
worker helper) to reuse across copies. After the batch finishes, nothing releases them —
they persist until the next `reuse_pages=False` command or helper exit. On a machine with
many workers, idle-time peak memory can be substantial.

**Fix options (pick one):**
- Parent sends an explicit `{"cmd": "release"}` after each batch completes
  (`WorkerRuntime._process_batch` end), and the helper clears `prepared`/`prepared_key`.
- Or, simpler: helper clears the cache after N seconds of stdin inactivity — but the
  explicit release command is more deterministic and keeps the protocol simple.

Keep the reuse behavior *within* a batch unchanged — it exists to avoid re-decoding the
cache per copy.

---

## Explicitly checked and NOT issues (do not "fix")

- **i18n coverage:** all 270 statically-used translation keys exist in both `en` and
  `zh-Hans`, including the dynamic key families (`spool.stage.*`, `settings.fit_mode.*`,
  `statistics.header.*`). Verified by script.
- **Maintenance stage-1 quiet-window race:** the gap between `_wait_for_resume` passing and
  `spool_send_start_callback()` incrementing the active-send counter is a few instructions
  wide; stage 2 (queue drain) backstops it. Deliberately not worth fixing.
- **Run lifecycle state machine** (`RUNNING`/`STOPPING`/`IDLE`), tail-balance accounting,
  force-stop/kill-inflight paths, statistics pending-run recovery: reviewed line-by-line, no
  defects found.
