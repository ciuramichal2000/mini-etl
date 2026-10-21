"""Command line: ``python -m minietl run config.json [--dry-run]``."""

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from .config import load_config_file
from .errors import MiniEtlError
from .models import RunResult
from .pipeline import run_pipeline

MAX_REJECTS_SHOWN = 10


def format_result(result: RunResult) -> str:
    """Human-readable run summary; lists the first rejects so problems are visible at once."""
    mode = "dry run, nothing written" if result.dry_run else f"run {result.run_id}"
    lines = [
        f"pipeline {result.pipeline} ({mode})",
        f"read {result.rows_read}: inserted {result.rows_inserted}, updated {result.rows_updated}, "
        f"unchanged {result.rows_unchanged}, filtered {result.rows_filtered}, rejected {result.rows_rejected}",
    ]
    for reject in result.rejects[:MAX_REJECTS_SHOWN]:
        lines.append(f"  rejected line {reject.line_number} [{reject.stage}]: {reject.reason}")
    hidden = len(result.rejects) - MAX_REJECTS_SHOWN
    if hidden > 0:
        where = "a dry run does not store them" if result.dry_run else "see table etl_rejects"
        lines.append(f"  ... and {hidden} more ({where})")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m minietl", description="Config-driven CSV to SQLite ETL.")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run the pipeline described by a JSON config file")
    run.add_argument("config", type=Path, help="path to the JSON config; relative paths inside it resolve next to it")
    run.add_argument("--dry-run", action="store_true", help="classify every row but write nothing")
    args = parser.parse_args(argv)

    try:
        config = load_config_file(args.config)
        result = run_pipeline(config, base_dir=args.config.resolve().parent, dry_run=args.dry_run)
    except MiniEtlError as error:
        print(f"error: {error}", file=sys.stderr)
        for note in getattr(error, "__notes__", []):
            print(f"note: {note}", file=sys.stderr)
        return 1
    print(format_result(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
