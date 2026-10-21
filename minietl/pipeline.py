"""Orchestration: extract -> transform -> load, with the reject/abort policy in one place.

Policy: a problem with ONE row (bad value, unmatched lookup, duplicate key,
wrong field count) rejects that row and the run continues. A problem with the
SETUP (unknown column in a step, unreadable file, incompatible target table)
aborts the run, rolls the load back and, for real runs, leaves a 'failed' row
in the run log.
"""

from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .config import PipelineConfig, parse_config
from .csvsource import CsvTable, SourceRow, open_csv
from .errors import ConfigError, LoadError, MiniEtlError, RowError
from .models import Reject, RunResult
from .steps import Row, Schema, Step
from .target import INSERTED, UNCHANGED, UPDATED, Target, record_failure

READ, FILTERED, REJECTED = "read", "filtered", "rejected"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(moment: datetime) -> str:
    return moment.isoformat(timespec="seconds")


def run_pipeline(
    config: Mapping[str, Any],
    *,
    base_dir: str | Path | None = None,
    dry_run: bool = False,
    clock: Callable[[], datetime] = _utc_now,
) -> RunResult:
    """Run the pipeline described by ``config`` and return what happened.

    ``clock`` is called once at the start and once at the end so tests can
    inject fixed times (a run that is aborted after its last row, while the log
    is being written, reads it one more time for the failed log row). With
    ``dry_run`` nothing is written: the database is opened read-only, and
    neither data nor run log nor rejects are stored.
    Raises a MiniEtlError subclass when the run cannot proceed at all.
    """
    pipeline = parse_config(config, base_dir)
    started_at = _timestamp(clock())
    try:
        return _execute(pipeline, dry_run, started_at, clock)
    except MiniEtlError as error:
        if not dry_run:
            _log_failure(pipeline, started_at, _timestamp(clock()), error)
        raise


def _log_failure(pipeline: PipelineConfig, started_at: str, finished_at: str, error: MiniEtlError) -> None:
    try:
        record_failure(
            pipeline.target.database,
            pipeline=pipeline.name,
            source=str(pipeline.source.path),
            table=pipeline.target.table,
            started_at=started_at,
            finished_at=finished_at,
            error=f"{type(error).__name__}: {error}",
        )
    except LoadError as log_error:
        # The original error matters more; keep the logging failure visible alongside it.
        error.add_note(str(log_error))


def _bind_steps(pipeline: PipelineConfig, header: Sequence[str]) -> Schema:
    """Compute the final column layout, failing early on any bad column reference."""
    schema: Schema = {name: "TEXT" for name in header}
    for step in pipeline.steps:
        schema = step.bind(schema)
    if pipeline.target.key not in schema:
        raise ConfigError(
            f"target key {pipeline.target.key!r} is not a column after the steps; "
            f"columns: {list(schema)}"
        )
    return schema


def _apply_steps(steps: Sequence[Step], record: Row) -> Row | None:
    """Run the chain; None means a filter dropped the row."""
    row: Row | None = record
    for step in steps:
        try:
            row = step.apply(row)
        except RowError as error:
            error.stage = error.stage or step.label
            raise
        if row is None:
            return None
    return row


def _process_row(
    source_row: SourceRow,
    table: CsvTable,
    steps: Sequence[Step],
    key: str,
    seen_keys: dict[Any, int],
    target: Target,
) -> str:
    """Take one CSV line through transform and load; return its outcome bucket."""
    row = _apply_steps(steps, table.to_record(source_row))
    if row is None:
        return FILTERED
    key_value = row[key]
    if key_value is None:
        raise RowError(f"key column {key!r} is empty", stage="load")
    if key_value in seen_keys:
        raise RowError(
            f"duplicate key {key_value!r}; first seen on line {seen_keys[key_value]}", stage="load"
        )
    outcome = target.upsert(row)
    # Only a row that made it through reserves its key: a rejected row must not turn the
    # next row with the same key into a "duplicate".
    seen_keys[key_value] = source_row.line_number
    return outcome


def _execute(
    pipeline: PipelineConfig, dry_run: bool, started_at: str, clock: Callable[[], datetime]
) -> RunResult:
    source = pipeline.source
    tally: Counter[str] = Counter()
    rejects: list[Reject] = []
    with open_csv(source.path, delimiter=source.delimiter, encoding=source.encoding) as table:
        schema = _bind_steps(pipeline, table.header)
        with Target(
            pipeline.target.database, pipeline.target.table, pipeline.target.key, schema, dry_run=dry_run
        ) as target:
            seen_keys: dict[Any, int] = {}
            for source_row in table.rows():
                tally[READ] += 1
                try:
                    outcome = _process_row(
                        source_row, table, pipeline.steps, pipeline.target.key, seen_keys, target
                    )
                except RowError as error:
                    tally[REJECTED] += 1
                    rejects.append(
                        Reject(source_row.line_number, error.stage or "row", error.reason, source_row.cells)
                    )
                else:
                    tally[outcome] += 1
            result = RunResult(
                run_id=None,
                pipeline=pipeline.name,
                source=str(source.path),
                table=pipeline.target.table,
                dry_run=dry_run,
                started_at=started_at,
                finished_at=_timestamp(clock()),
                rows_read=tally[READ],
                rows_inserted=tally[INSERTED],
                rows_updated=tally[UPDATED],
                rows_unchanged=tally[UNCHANGED],
                rows_filtered=tally[FILTERED],
                rows_rejected=tally[REJECTED],
                rejects=tuple(rejects),
            )
            if dry_run:
                return result
            run_id = target.record_run(result)
            target.commit()
            return replace(result, run_id=run_id)
