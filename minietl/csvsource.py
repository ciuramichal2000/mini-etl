"""CSV extraction: header validation, line numbers and per-row width checks.

File-level problems (missing file, no header, duplicate column names,
undecodable bytes) abort the run. A single short or long line only rejects
that line.
"""

import csv
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .errors import RowError, SourceError


class _LineReader(Protocol):
    """The part of csv.reader this module relies on (its class is private)."""

    line_num: int

    def __iter__(self) -> Iterator[list[str]]: ...

    def __next__(self) -> list[str]: ...


@dataclass(frozen=True)
class SourceRow:
    """One data line: where it ends in the file and its raw cell texts."""

    line_number: int
    cells: tuple[str, ...]


class CsvTable:
    """An open CSV file whose header has already been validated."""

    def __init__(self, path: Path, header: list[str], reader: _LineReader) -> None:
        self.path = path
        self.header = header
        self._reader = reader

    def rows(self) -> Iterator[SourceRow]:
        """Yield data lines, skipping blank ones.

        Malformed CSV or undecodable bytes raise SourceError: past that point
        the file cannot be trusted, so rejecting "just that row" would be a guess.
        """
        try:
            for cells in self._reader:
                if cells:
                    yield SourceRow(self._reader.line_num, tuple(cells))
        except (csv.Error, UnicodeDecodeError) as exc:
            raise SourceError(f"{self.path}: unreadable after line {self._reader.line_num}: {exc}") from exc

    def to_record(self, row: SourceRow) -> dict[str, str | None]:
        """Pair cells with column names; blank cells become None (SQL NULL).

        Cells are stripped because stray spaces around CSV values are almost
        always noise, and a width mismatch rejects the row.
        """
        if len(row.cells) != len(self.header):
            raise RowError(
                f"expected {len(self.header)} fields, found {len(row.cells)}",
                stage="extract",
            )
        return {name: cell.strip() or None for name, cell in zip(self.header, row.cells)}


def _read_header(reader: _LineReader, path: Path) -> list[str]:
    """Return the first non-blank line as validated column names."""
    header: list[str] | None = None
    try:
        for cells in reader:
            if cells:
                header = cells
                break
    except (csv.Error, UnicodeDecodeError) as exc:
        raise SourceError(f"{path}: cannot read the header row: {exc}") from exc
    if header is None:
        raise SourceError(f"{path}: file is empty, a header row is required")
    names = [name.strip() for name in header]
    if any(not name for name in names):
        raise SourceError(f"{path}: header contains an empty column name")
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise SourceError(f"{path}: duplicate column name(s) {duplicates}")
    return names


@contextmanager
def open_csv(
    path: Path, *, delimiter: str = ",", encoding: str = "utf-8-sig"
) -> Iterator[CsvTable]:
    """Open a CSV file for the duration of a ``with`` block."""
    try:
        handle = open(path, newline="", encoding=encoding)
    except OSError as exc:
        raise SourceError(f"cannot open CSV file {path}: {exc.strerror or exc}") from exc
    with handle:
        reader = csv.reader(handle, delimiter=delimiter)
        yield CsvTable(path, _read_header(reader, path), reader)
