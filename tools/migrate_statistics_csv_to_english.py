from __future__ import annotations

import argparse
import csv
import re
import shutil
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from printfarm.statistics_writer import CSV_HEADER, MonthlyStatisticsWriter  # noqa: E402


MONTHLY_CSV_RE = re.compile(r"^\d{4}-\d{2}\.csv$")


def backup_path_for(path: Path) -> Path:
    candidate = path.with_name(f"{path.name}.bak")
    if not candidate.exists():
        return candidate
    index = 1
    while True:
        candidate = path.with_name(f"{path.name}.bak{index}")
        if not candidate.exists():
            return candidate
        index += 1


def read_csv_rows(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return [row for row in csv.reader(fh)]


def migrate_csv(path: Path, statistics_dir: Path, dry_run: bool, backup: bool) -> str:
    writer_dir = statistics_dir if path.parent.resolve() == statistics_dir else path.parent.resolve()
    writer = MonthlyStatisticsWriter(writer_dir)
    rows = read_csv_rows(path)
    normalized_rows = writer._normalize_csv_rows(rows)
    output_rows = [CSV_HEADER] + normalized_rows
    if rows == output_rows:
        return f"unchanged: {path}"

    if dry_run:
        return f"would migrate: {path}"

    if backup:
        backup_path = backup_path_for(path)
        shutil.copy2(path, backup_path)
    writer._write_csv_rows(path, output_rows)
    return f"migrated: {path}"


def selected_csv_paths(statistics_dir: Path, paths: list[Path]) -> list[Path]:
    if paths:
        return [path.resolve() for path in paths]
    return sorted(path for path in statistics_dir.glob("*.csv") if MONTHLY_CSV_RE.fullmatch(path.name))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert InkSwarm statistics CSV files to the English CSV schema.",
    )
    parser.add_argument(
        "csv_paths",
        nargs="*",
        type=Path,
        help="Optional CSV files to migrate. Defaults to all CSV files in the statistics directory.",
    )
    parser.add_argument(
        "--statistics-dir",
        type=Path,
        default=REPO_ROOT / "statistics",
        help="Statistics directory used for default discovery and atomic rewrites.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without writing files.")
    parser.add_argument("--no-backup", action="store_true", help="Do not create .bak files before rewriting.")
    args = parser.parse_args()

    statistics_dir = args.statistics_dir.resolve()
    if not args.csv_paths and not statistics_dir.exists():
        print(f"statistics directory does not exist: {statistics_dir}", file=sys.stderr)
        return 2

    csv_paths = selected_csv_paths(statistics_dir, args.csv_paths)
    if not csv_paths:
        print(f"no CSV files found in {statistics_dir}")
        return 0

    for path in csv_paths:
        if not path.exists():
            print(f"missing: {path}", file=sys.stderr)
            continue
        print(migrate_csv(path, statistics_dir, dry_run=args.dry_run, backup=not args.no_backup))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
