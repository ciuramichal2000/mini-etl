"""Plain result objects shared by the pipeline, the SQLite target and the CLI."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Reject:
    """A row that was set aside, with enough context to find and fix it."""

    line_number: int
    stage: str
    reason: str
    cells: tuple[str, ...]


@dataclass(frozen=True)
class RunResult:
    """Outcome of one run.

    Every row read ends up in exactly one bucket, so
    ``rows_read == rows_inserted + rows_updated + rows_unchanged
    + rows_filtered + rows_rejected``. ``run_id`` is None for a dry run, which
    never touches the database (not even the run log).
    """

    run_id: int | None
    pipeline: str
    source: str
    table: str
    dry_run: bool
    started_at: str
    finished_at: str
    rows_read: int
    rows_inserted: int
    rows_updated: int
    rows_unchanged: int
    rows_filtered: int
    rows_rejected: int
    rejects: tuple[Reject, ...]

    @property
    def rows_loaded(self) -> int:
        """Rows actually written: new rows plus rows whose values changed."""
        return self.rows_inserted + self.rows_updated
