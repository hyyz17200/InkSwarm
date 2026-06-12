from __future__ import annotations

import argparse
from pathlib import Path

from .spooler_service import run_spooler_maintenance_helper


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="InkSwarm Print Spooler maintenance helper")
    parser.add_argument("--result-file", required=True)
    parser.add_argument("--events-file", required=True)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--language", default="en")
    args = parser.parse_args(argv)

    return run_spooler_maintenance_helper(
        result_file=Path(args.result_file),
        events_file=Path(args.events_file),
        timeout_seconds=args.timeout,
        language=args.language,
    )
