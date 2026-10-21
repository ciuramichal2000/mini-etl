"""Turning a JSON-serialisable dict (or file) into a validated pipeline definition."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import ConfigError
from .specs import check_keys, get_delimiter, get_encoding, get_str
from .steps import Step, build_step
from .target import RESERVED_TABLES


@dataclass(frozen=True)
class SourceConfig:
    path: Path
    delimiter: str
    encoding: str


@dataclass(frozen=True)
class TargetConfig:
    database: Path
    table: str
    key: str


@dataclass(frozen=True)
class PipelineConfig:
    name: str
    source: SourceConfig
    steps: tuple[Step, ...]
    target: TargetConfig


def load_config_file(path: str | Path) -> dict[str, Any]:
    """Read a JSON config file; the result is what ``parse_config`` expects."""
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError as exc:
        raise ConfigError(f"cannot read config file {path}: {exc.strerror or exc}") from exc
    except UnicodeDecodeError as exc:
        raise ConfigError(f"config file {path} is not valid UTF-8 text: {exc}") from exc
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"config file {path} is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"config file {path} must contain a JSON object")
    return data


def parse_config(raw: Any, base_dir: str | Path | None = None) -> PipelineConfig:
    """Validate ``raw`` and build the steps.

    Relative file paths (source, lookup files, database) resolve against
    ``base_dir`` (default: the current directory), so a config file behaves the
    same wherever it is launched from. Lookup files are read here.
    """
    base = Path(base_dir) if base_dir is not None else Path.cwd()
    data = check_keys("config", raw, required=("source", "target"), optional=("name", "steps"))

    source_data = check_keys("source", data["source"], required=("path",), optional=("delimiter", "encoding"))
    source = SourceConfig(
        path=base / get_str("source", source_data, "path"),
        delimiter=get_delimiter("source", source_data),
        encoding=get_encoding("source", source_data),
    )

    target_data = check_keys("target", data["target"], required=("database", "table", "key"))
    table = get_str("target", target_data, "table")
    if table.lower() in RESERVED_TABLES or table.lower().startswith("sqlite_"):
        raise ConfigError(f"target: table name {table!r} is reserved")
    target = TargetConfig(
        database=base / get_str("target", target_data, "database"),
        table=table,
        key=get_str("target", target_data, "key"),
    )

    step_specs = data.get("steps", [])
    if not isinstance(step_specs, list):
        raise ConfigError("steps must be a list")
    steps = tuple(build_step(spec, index, base) for index, spec in enumerate(step_specs, start=1))

    name = get_str("config", data, "name", default=table)
    return PipelineConfig(name=name, source=source, steps=steps, target=target)
