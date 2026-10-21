"""Validation helpers for the JSON-serialisable configuration.

Config typos are the most likely user error, so unknown keys are rejected
instead of silently ignored ("delimeter" must not quietly fall back to a comma).
"""

import codecs
from collections.abc import Iterable
from typing import Any

from .errors import ConfigError


def check_keys(
    where: str,
    data: Any,
    required: Iterable[str] = (),
    optional: Iterable[str] = (),
) -> dict[str, Any]:
    """Require an object with exactly the allowed keys."""
    if not isinstance(data, dict):
        raise ConfigError(f"{where} must be an object")
    required = tuple(required)
    missing = [key for key in required if key not in data]
    if missing:
        raise ConfigError(f"{where}: missing key(s) {missing}")
    allowed = set(required) | set(optional)
    unknown = sorted(str(key) for key in data if key not in allowed)
    if unknown:
        raise ConfigError(f"{where}: unknown key(s) {unknown}; allowed: {sorted(allowed)}")
    return data


def get_str(where: str, data: dict[str, Any], key: str, default: str | None = None) -> str:
    """Read a non-empty string value."""
    value = data.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where}: '{key}' must be a non-empty string")
    return value


def get_choice(
    where: str,
    data: dict[str, Any],
    key: str,
    choices: Iterable[str],
    default: str | None,
) -> str:
    """Read a string that must be one of a fixed set."""
    allowed = tuple(choices)
    value = data.get(key, default)
    if not isinstance(value, str) or value not in allowed:
        raise ConfigError(f"{where}: '{key}' must be one of {sorted(allowed)}, got {value!r}")
    return value


def str_mapping(where: str, value: Any) -> dict[str, str]:
    """Read a non-empty {name: name} object."""
    if not isinstance(value, dict) or not value:
        raise ConfigError(f"{where} must be a non-empty object of strings")
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip() or not isinstance(item, str) or not item.strip():
            raise ConfigError(f"{where}: keys and values must be non-empty strings (got {key!r}: {item!r})")
    return dict(value)


def get_delimiter(where: str, data: dict[str, Any]) -> str:
    """Read a one-character CSV delimiter (not a line break, not the quote character)."""
    value = data.get("delimiter", ",")
    if not isinstance(value, str) or len(value) != 1 or value in '\r\n"':
        raise ConfigError(
            f"{where}: 'delimiter' must be a single character other than a line break or '\"', got {value!r}"
        )
    return value


def get_encoding(where: str, data: dict[str, Any]) -> str:
    """Read a text encoding name, defaulting to UTF-8 that also strips an Excel BOM."""
    value = data.get("encoding", "utf-8-sig")
    try:
        codecs.lookup(value)
        # Byte-to-byte and text-to-text codecs ("hex", "rot13") are registered too, but
        # open() cannot use them; str.encode/bytes.decode refuse exactly those.
        "".encode(value)
        b"".decode(value)
    except (LookupError, TypeError, ValueError) as exc:
        raise ConfigError(f"{where}: {value!r} is not a usable text encoding") from exc
    return value
