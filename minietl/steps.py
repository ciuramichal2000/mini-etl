"""Transform steps: rename, cast, filter, derive and lookup.

Every step does two jobs. ``bind`` describes how the step changes the column
layout; the pipeline calls it once before reading any row, so a typo in the
config fails the whole run immediately. ``apply`` transforms one row; there a
bad value only rejects that row (RowError).
"""

import math
import re
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .csvsource import open_csv
from .errors import ConfigError, RowError, SourceError
from .expressions import Expression
from .specs import check_keys, get_choice, get_delimiter, get_encoding, get_str, str_mapping

Row = dict[str, Any]
Schema = dict[str, str]  # column name -> SQLite declared type ("" means no type affinity)

SQL_TYPES = {"int": "INTEGER", "float": "REAL", "str": "TEXT", "bool": "INTEGER", "date": "TEXT"}
DEFAULT_DATE_FORMAT = "%Y-%m-%d"

_INT64 = 2**63
# int()/float() also accept "1_000", "nan" and "infinity"; for data loading those are
# almost certainly mistakes, so the accepted spelling is spelled out explicitly.
_INT_PATTERN = re.compile(r"[+-]?[0-9]+")
_FLOAT_PATTERN = re.compile(r"[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?")
_TRUE_WORDS = frozenset({"true", "yes", "y", "1"})
_FALSE_WORDS = frozenset({"false", "no", "n", "0"})


def check_storable(value: Any) -> Any:
    """Return ``value`` if SQLite can store it faithfully, else reject the row.

    A value SQLite cannot bind (a tuple), or silently mangles (NaN becomes NULL,
    a 65-bit integer overflows) would otherwise abort the whole load midway.
    """
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        if not -_INT64 <= value < _INT64:
            raise RowError(f"integer {value} does not fit in 64 bits")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RowError(f"{value} is not a finite number")
        return value
    raise RowError(f"cannot store a value of type {type(value).__name__}")


@dataclass(frozen=True)
class CastSpec:
    """Target type for one column; the format is only used for dates."""

    to: str
    date_format: str = DEFAULT_DATE_FORMAT


def cast_value(value: Any, spec: CastSpec) -> Any:
    """Convert one value strictly; None stays None."""
    if value is None:
        return None
    if spec.to == "str":
        return value if isinstance(value, str) else str(value)
    text = (value if isinstance(value, str) else str(value)).strip()
    if spec.to == "int":
        if not _INT_PATTERN.fullmatch(text):
            raise ValueError(f"{text!r} is not an integer")
        number = int(text)
        if not -_INT64 <= number < _INT64:
            raise ValueError(f"{text!r} does not fit in 64 bits")
        return number
    if spec.to == "float":
        if not _FLOAT_PATTERN.fullmatch(text):
            raise ValueError(f"{text!r} is not a number")
        number = float(text)
        if not math.isfinite(number):
            raise ValueError(f"{text!r} is out of range for a float")
        return number
    if spec.to == "bool":
        word = text.lower()
        if word in _TRUE_WORDS:
            return True
        if word in _FALSE_WORDS:
            return False
        raise ValueError(f"{text!r} is not a boolean (use true/false, yes/no or 1/0)")
    try:
        return datetime.strptime(text, spec.date_format).date().isoformat()
    except ValueError as exc:
        raise ValueError(f"{text!r} is not a date in format {spec.date_format!r}") from exc


def _require_columns(label: str, wanted: Iterable[str], schema: Schema) -> None:
    missing = [column for column in wanted if column not in schema]
    if missing:
        raise ConfigError(f"{label}: unknown column(s) {missing}; available: {list(schema)}")


class Step(ABC):
    """One stage of the transform chain."""

    def __init__(self, label: str) -> None:
        self.label = label

    @abstractmethod
    def bind(self, schema: Schema) -> Schema:
        """Check this step against the incoming columns and return the outgoing ones."""

    @abstractmethod
    def apply(self, row: Row) -> Row | None:
        """Return the transformed row, or None to drop it (raise RowError to reject it)."""


class RenameStep(Step):
    def __init__(self, label: str, mapping: dict[str, str]) -> None:
        super().__init__(label)
        self.mapping = mapping

    def bind(self, schema: Schema) -> Schema:
        _require_columns(self.label, self.mapping, schema)
        renamed = {self.mapping.get(column, column): sql for column, sql in schema.items()}
        if len(renamed) != len(schema):
            raise ConfigError(f"{self.label}: renaming would give two columns the same name")
        return renamed

    def apply(self, row: Row) -> Row:
        return {self.mapping.get(column, column): value for column, value in row.items()}


class CastStep(Step):
    def __init__(self, label: str, casts: dict[str, CastSpec]) -> None:
        super().__init__(label)
        self.casts = casts

    def bind(self, schema: Schema) -> Schema:
        _require_columns(self.label, self.casts, schema)
        typed = dict(schema)
        for column, spec in self.casts.items():
            typed[column] = SQL_TYPES[spec.to]
        return typed

    def apply(self, row: Row) -> Row:
        converted = dict(row)
        for column, spec in self.casts.items():
            try:
                converted[column] = cast_value(row[column], spec)
            except ValueError as exc:
                raise RowError(f"column {column!r}: {exc}") from exc
        return converted


class FilterStep(Step):
    """Keep rows where the expression is true.

    A false row is dropped by default (it is not an error, so it is counted as
    "filtered"); with ``reject_failures`` it becomes a reject, which is what
    validation rules want.
    """

    def __init__(self, label: str, condition: Expression, reject_failures: bool) -> None:
        super().__init__(label)
        self.condition = condition
        self.reject_failures = reject_failures

    def bind(self, schema: Schema) -> Schema:
        _require_columns(self.label, sorted(self.condition.names), schema)
        return schema

    def apply(self, row: Row) -> Row | None:
        if self.condition.evaluate(row):
            return row
        if self.reject_failures:
            raise RowError(f"condition not met: {self.condition.text}")
        return None


class DeriveStep(Step):
    def __init__(self, label: str, column: str, expression: Expression) -> None:
        super().__init__(label)
        self.column = column
        self.expression = expression

    def bind(self, schema: Schema) -> Schema:
        _require_columns(self.label, sorted(self.expression.names), schema)
        derived = dict(schema)
        # Computed values have no fixed type (3 / 2 is a float, 3 // 2 an int), so the
        # column gets no affinity and SQLite keeps each value as computed.
        derived[self.column] = ""
        return derived

    def apply(self, row: Row) -> Row:
        derived = dict(row)
        derived[self.column] = check_storable(self.expression.evaluate(row))
        return derived


class LookupStep(Step):
    """Left or inner join against a second CSV, held in memory as a dict."""

    def __init__(
        self,
        label: str,
        on: str,
        outputs: dict[str, str],
        reject_missing: bool,
        table: dict[str, dict[str, str | None]],
        source_name: str,
    ) -> None:
        super().__init__(label)
        self.on = on
        self.outputs = outputs  # new column name -> column in the lookup file
        self.reject_missing = reject_missing
        self.table = table
        self.source_name = source_name

    def bind(self, schema: Schema) -> Schema:
        _require_columns(self.label, [self.on], schema)
        clashes = [column for column in self.outputs if column in schema]
        if clashes:
            raise ConfigError(f"{self.label}: lookup would overwrite existing column(s) {clashes}")
        joined = dict(schema)
        for column in self.outputs:
            joined[column] = "TEXT"
        return joined

    def apply(self, row: Row) -> Row:
        value = row[self.on]
        # Lookup keys come from a CSV and are therefore text; match on the text form
        # so an integer id (after a cast step) still finds its "42" in the file.
        match = None if value is None else self.table.get(str(value))
        if match is None and self.reject_missing:
            raise RowError(f"no match for {self.on}={value!r} in {self.source_name}")
        joined = dict(row)
        for column, source_column in self.outputs.items():
            joined[column] = None if match is None else match[source_column]
        return joined


def _load_lookup_table(
    label: str,
    path: Path,
    key: str,
    wanted: Iterable[str],
    delimiter: str,
    encoding: str,
) -> dict[str, dict[str, str | None]]:
    """Read the reference CSV; duplicate keys would make the join ambiguous, so they abort."""
    wanted = list(wanted)
    table: dict[str, dict[str, str | None]] = {}
    with open_csv(path, delimiter=delimiter, encoding=encoding) as csv_table:
        missing = [column for column in (key, *wanted) if column not in csv_table.header]
        if missing:
            raise ConfigError(f"{label}: lookup file {path.name} has no column(s) {missing}")
        for source_row in csv_table.rows():
            where = f"{label}: lookup file {path.name} line {source_row.line_number}"
            try:
                record = csv_table.to_record(source_row)
            except RowError as exc:
                raise SourceError(f"{where}: {exc.reason}") from exc
            key_value = record[key]
            if key_value is None:
                raise SourceError(f"{where}: empty key in column {key!r}")
            if key_value in table:
                raise SourceError(f"{where}: duplicate key {key_value!r}")
            table[key_value] = {column: record[column] for column in wanted}
    return table


def _build_rename(label: str, spec: dict[str, Any], base_dir: Path) -> Step:
    data = check_keys(label, spec, required=("type", "mapping"))
    return RenameStep(label, str_mapping(f"{label} 'mapping'", data["mapping"]))


def _check_date_format(where: str, date_format: str) -> None:
    """Refuse a format strptime cannot use, so a typo fails once instead of rejecting every row."""
    sample = datetime(2001, 2, 3, 4, 5, 6, tzinfo=timezone.utc)  # aware, so %z and %Z round-trip too
    try:
        datetime.strptime(sample.strftime(date_format), date_format)
    except ValueError as exc:
        raise ConfigError(f"{where}: {date_format!r} is not a usable date format ({exc})") from exc


def _build_cast(label: str, spec: dict[str, Any], base_dir: Path) -> Step:
    data = check_keys(label, spec, required=("type", "columns"))
    columns = data["columns"]
    if not isinstance(columns, dict) or not columns:
        raise ConfigError(f"{label}: 'columns' must be a non-empty object")
    casts: dict[str, CastSpec] = {}
    for column, target in columns.items():
        where = f"{label} column {column!r}"
        if isinstance(target, str):
            target = {"to": target}
        target = check_keys(where, target, required=("to",), optional=("format",))
        to = get_choice(where, target, "to", SQL_TYPES, None)
        date_format = DEFAULT_DATE_FORMAT
        if "format" in target:
            if to != "date":
                raise ConfigError(f"{where}: 'format' only applies to date")
            date_format = get_str(where, target, "format")
        if to == "date":
            _check_date_format(where, date_format)
        casts[column] = CastSpec(to, date_format)
    return CastStep(label, casts)


def _build_filter(label: str, spec: dict[str, Any], base_dir: Path) -> Step:
    data = check_keys(label, spec, required=("type", "expr"), optional=("on_false",))
    on_false = get_choice(label, data, "on_false", ("drop", "reject"), "drop")
    return FilterStep(label, Expression(data["expr"]), reject_failures=on_false == "reject")


def _build_derive(label: str, spec: dict[str, Any], base_dir: Path) -> Step:
    data = check_keys(label, spec, required=("type", "column", "expr"))
    return DeriveStep(label, get_str(label, data, "column"), Expression(data["expr"]))


def _build_lookup(label: str, spec: dict[str, Any], base_dir: Path) -> Step:
    data = check_keys(
        label,
        spec,
        required=("type", "path", "on", "key", "columns"),
        optional=("on_missing", "delimiter", "encoding"),
    )
    path = base_dir / get_str(label, data, "path")
    outputs = str_mapping(f"{label} 'columns'", data["columns"])
    on_missing = get_choice(label, data, "on_missing", ("reject", "null"), "reject")
    table = _load_lookup_table(
        label,
        path,
        get_str(label, data, "key"),
        outputs.values(),
        get_delimiter(label, data),
        get_encoding(label, data),
    )
    return LookupStep(
        label,
        get_str(label, data, "on"),
        outputs,
        reject_missing=on_missing == "reject",
        table=table,
        source_name=path.name,
    )


_BUILDERS: dict[str, Callable[[str, dict[str, Any], Path], Step]] = {
    "rename": _build_rename,
    "cast": _build_cast,
    "filter": _build_filter,
    "derive": _build_derive,
    "lookup": _build_lookup,
}


def build_step(spec: Any, index: int, base_dir: Path) -> Step:
    """Create a step from its config object; ``index`` (1-based) labels errors and rejects."""
    if not isinstance(spec, dict):
        raise ConfigError(f"step {index} must be an object")
    kind = spec.get("type")
    if not isinstance(kind, str) or kind not in _BUILDERS:
        raise ConfigError(f"step {index}: unknown type {kind!r}; expected one of {sorted(_BUILDERS)}")
    return _BUILDERS[kind](f"step {index} ({kind})", spec, base_dir)
