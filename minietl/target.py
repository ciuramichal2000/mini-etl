"""SQLite destination: table checks, idempotent upsert and the run log.

A real run writes data, run-log row and rejects inside ONE transaction, so it
either leaves a complete, consistent record or (after a rollback) nothing but a
'failed' log row written separately. A dry run opens the database read-only
(or not at all when it does not exist) and only classifies what would happen.
"""

import contextlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from .errors import LoadError, RowError
from .models import RunResult
from .steps import Row, Schema

RUN_LOG_TABLE = "etl_run_log"
REJECT_TABLE = "etl_rejects"
RESERVED_TABLES = frozenset({RUN_LOG_TABLE, REJECT_TABLE})

INSERTED, UPDATED, UNCHANGED = "inserted", "updated", "unchanged"

_LOG_DDL = (
    f"""CREATE TABLE IF NOT EXISTS {RUN_LOG_TABLE} (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        pipeline TEXT NOT NULL,
        source_path TEXT NOT NULL,
        target_table TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('success', 'failed')),
        started_at TEXT NOT NULL,
        finished_at TEXT NOT NULL,
        rows_read INTEGER,
        rows_loaded INTEGER,
        rows_inserted INTEGER,
        rows_updated INTEGER,
        rows_unchanged INTEGER,
        rows_filtered INTEGER,
        rows_rejected INTEGER,
        error TEXT
    )""",
    f"""CREATE TABLE IF NOT EXISTS {REJECT_TABLE} (
        reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL REFERENCES {RUN_LOG_TABLE}(run_id),
        line_number INTEGER NOT NULL,
        stage TEXT NOT NULL,
        reason TEXT NOT NULL,
        row_json TEXT NOT NULL
    )""",
)

_INSERT_RUN = (
    f"INSERT INTO {RUN_LOG_TABLE} (pipeline, source_path, target_table, status, started_at, "
    "finished_at, rows_read, rows_loaded, rows_inserted, rows_updated, rows_unchanged, "
    "rows_filtered, rows_rejected, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)
_INSERT_REJECT = (
    f"INSERT INTO {REJECT_TABLE} (run_id, line_number, stage, reason, row_json) VALUES (?, ?, ?, ?, ?)"
)


def quote_identifier(name: str) -> str:
    """Quote a table/column name so arbitrary CSV headers are safe inside SQL."""
    return '"' + name.replace('"', '""') + '"'


class Target:
    """The SQLite table a pipeline loads into, used as a context manager."""

    def __init__(self, database: Path, table: str, key: str, schema: Schema, *, dry_run: bool) -> None:
        self.database = database
        self.table = table
        self.key = key
        self.dry_run = dry_run
        self._schema = schema
        self._columns = list(schema)
        self._non_key = [column for column in self._columns if column != key]
        self._conn: sqlite3.Connection | None = None
        self._table_exists = False

        quote, table_sql = quote_identifier, quote_identifier(table)
        key_test = f"{quote(key)} = ?"
        column_list = ", ".join(quote(column) for column in self._columns)
        placeholders = ", ".join("?" for _ in self._columns)
        assignments = ", ".join(f"{quote(column)} = ?" for column in self._non_key)
        same_tests = "".join(f" AND {quote(column)} IS ?" for column in self._non_key)
        self._sql_exists = f"SELECT 1 FROM {table_sql} WHERE {key_test}"
        # IS (not =) so NULL matches NULL; SQLite also applies the column's type affinity
        # to the parameter, so '5' stored in a TEXT column still equals the value 5.
        self._sql_same = f"{self._sql_exists}{same_tests}"
        self._sql_insert = f"INSERT INTO {table_sql} ({column_list}) VALUES ({placeholders})"
        # A table with only the key column can never differ from an existing row, so the
        # UPDATE (which would have nothing to set) is only built when it can be reached.
        self._sql_update = f"UPDATE {table_sql} SET {assignments} WHERE {key_test}" if self._non_key else ""

    def __enter__(self) -> "Target":
        try:
            if self.dry_run:
                self._open_read_only()
            else:
                self._open_for_write()
        except sqlite3.Error as exc:
            self._close()
            raise LoadError(f"cannot open database {self.database}: {exc}") from exc
        except LoadError:
            self._close()
            raise
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self._conn is not None and self._conn.in_transaction:
            with contextlib.suppress(sqlite3.Error):  # closing the connection rolls back anyway
                self._conn.execute("ROLLBACK")
        self._close()

    def _close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _open_for_write(self) -> None:
        self._conn = sqlite3.connect(self.database, isolation_level=None)
        self._conn.execute("BEGIN IMMEDIATE")
        for statement in _LOG_DDL:
            self._conn.execute(statement)
        self._prepare_table()

    def _open_read_only(self) -> None:
        if not self.database.exists():
            if not self.database.parent.is_dir():  # the real run could not create the file either
                raise LoadError(f"cannot open database {self.database}: its folder does not exist")
            return  # nothing to compare against: every row would be an insert
        uri = f"{self.database.resolve().as_uri()}?mode=ro"
        self._conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        self._prepare_table()

    def _prepare_table(self) -> None:
        """Create the table, or verify an existing one is the table this pipeline produces."""
        assert self._conn is not None
        info = self._conn.execute(f"PRAGMA table_info({quote_identifier(self.table)})").fetchall()
        if not info:
            if not self.dry_run:
                definitions = ", ".join(self._column_definition(column) for column in self._columns)
                self._conn.execute(
                    f"CREATE TABLE {quote_identifier(self.table)} "
                    f"({definitions}, PRIMARY KEY ({quote_identifier(self.key)}))"
                )
                self._table_exists = True
            return
        existing = {row[1] for row in info}
        if existing != set(self._columns):
            raise LoadError(
                f"table {self.table!r} has columns {sorted(existing)} but this pipeline "
                f"produces {sorted(self._columns)}"
            )
        primary_key = [row[1] for row in sorted(info, key=lambda row: row[5]) if row[5]]
        if primary_key != [self.key]:
            raise LoadError(
                f"table {self.table!r} must have {self.key!r} as its only primary key "
                f"(found {primary_key})"
            )
        self._table_exists = True

    def _column_definition(self, column: str) -> str:
        definition = f"{quote_identifier(column)} {self._schema[column]}".rstrip()
        return f"{definition} NOT NULL" if column == self.key else definition

    def upsert(self, row: Row) -> str:
        """Insert, update or leave alone the row with this key; report which.

        Looking the row up first (rather than INSERT OR REPLACE) is what lets a
        re-run report "unchanged" and never rewrite identical data.
        """
        if not self._table_exists:
            return INSERTED  # dry run against a database/table that does not exist yet
        assert self._conn is not None
        key_value = row[self.key]
        try:
            if self._conn.execute(self._sql_exists, (key_value,)).fetchone() is None:
                if not self.dry_run:
                    self._conn.execute(self._sql_insert, [row[column] for column in self._columns])
                return INSERTED
            values = [row[column] for column in self._non_key]
            if self._conn.execute(self._sql_same, [key_value, *values]).fetchone() is not None:
                return UNCHANGED
            if not self.dry_run:
                self._conn.execute(self._sql_update, [*values, key_value])
            return UPDATED
        except sqlite3.IntegrityError as exc:
            # NOT NULL / CHECK / UNIQUE / type-mismatch on a table somebody else defined: the
            # statement was rolled back on its own, so this row is bad data, not a broken load.
            raise RowError(f"the database refused the row: {exc}", stage="load") from exc
        except sqlite3.Error as exc:
            raise LoadError(f"cannot write row with {self.key}={key_value!r}: {exc}") from exc

    def record_run(self, result: RunResult) -> int:
        """Write the run-log row and its rejects (same transaction as the data)."""
        assert self._conn is not None and not self.dry_run
        try:
            cursor = self._conn.execute(
                _INSERT_RUN,
                (
                    result.pipeline,
                    result.source,
                    result.table,
                    "success",
                    result.started_at,
                    result.finished_at,
                    result.rows_read,
                    result.rows_loaded,
                    result.rows_inserted,
                    result.rows_updated,
                    result.rows_unchanged,
                    result.rows_filtered,
                    result.rows_rejected,
                    None,
                ),
            )
            run_id = cursor.lastrowid
            assert run_id is not None
            self._conn.executemany(
                _INSERT_REJECT,
                [
                    (run_id, reject.line_number, reject.stage, reject.reason,
                     json.dumps(list(reject.cells), ensure_ascii=False))
                    for reject in result.rejects
                ],
            )
        except sqlite3.Error as exc:
            raise LoadError(f"cannot write the run log: {exc}") from exc
        return run_id

    def commit(self) -> None:
        if self.dry_run or self._conn is None:
            return
        try:
            self._conn.execute("COMMIT")
        except sqlite3.Error as exc:
            raise LoadError(f"cannot commit: {exc}") from exc


def record_failure(
    database: Path,
    *,
    pipeline: str,
    source: str,
    table: str,
    started_at: str,
    finished_at: str,
    error: str,
) -> None:
    """Log an aborted run on its own connection, after the data transaction rolled back.

    Counts are NULL: the load was rolled back, so reporting partial numbers
    would suggest data that is not in the table.
    """
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(database, isolation_level=None)
        conn.execute("BEGIN IMMEDIATE")
        for statement in _LOG_DDL:
            conn.execute(statement)
        conn.execute(
            _INSERT_RUN,
            (pipeline, source, table, "failed", started_at, finished_at, *([None] * 7), error),
        )
        conn.execute("COMMIT")
    except sqlite3.Error as exc:
        raise LoadError(f"cannot write the failed run to the run log: {exc}") from exc
    finally:
        if conn is not None:
            conn.close()
