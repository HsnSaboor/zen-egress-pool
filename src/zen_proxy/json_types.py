"""Shared JSON value types for strict static typing.

Single home for recursive JSON aliases so no module repeats them.
Uses ``object``-narrowing helpers instead of ``Any`` (banned by repo gate).
"""

from __future__ import annotations

from typing import TypeGuard

JsonScalar = str | int | float | bool | None
JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonDict = dict[str, JsonValue]
JsonList = list[JsonValue]


def _is_scalar(value: object) -> TypeGuard[JsonScalar]:
    return value is None or isinstance(value, (str, int, float, bool))


def is_json_value(value: object) -> TypeGuard[JsonValue]:
    """Deep type guard for JSON-compatible values."""
    if _is_scalar(value):
        return True
    if isinstance(value, list):
        return all(is_json_value(entry) for entry in value)
    if isinstance(value, dict):
        return all(isinstance(key, str) and is_json_value(item) for key, item in value.items())
    return False


def as_json_dict(value: object) -> JsonDict | None:
    """Narrow an unknown decoded-JSON value to a JSON object, else None."""
    if not isinstance(value, dict):
        return None
    out: JsonDict = {}
    for key, item in value.items():
        if not isinstance(key, str):
            return None
        if not is_json_value(item):
            return None
        out[key] = item
    return out


def as_doc_dict(value: object) -> DocDict | None:
    """Narrow an unknown decoded value to a document dict (heterogeneous OK)."""
    if not isinstance(value, dict):
        return None
    out: DocDict = {}
    for key, item in value.items():
        if not isinstance(key, str):
            return None
        out[key] = item
    return out


def as_json_value(value: object) -> JsonValue | None:
    """Narrow an unknown decoded-JSON value to JsonValue (None when not JSON)."""
    if is_json_value(value):
        return value
    return None


def json_dict_from_unknown(value: object) -> JsonDict | None:
    """Alias kept short for call sites parsing request/response bodies."""
    return as_json_dict(value)


def str_field(event: JsonDict, name: str) -> str:
    """Read an optional string field (empty string when absent/non-string)."""
    raw = event.get(name)
    return raw if isinstance(raw, str) else ""


def dict_field(event: JsonDict, name: str) -> JsonDict | None:
    """Read an optional object field."""
    return as_json_dict(event.get(name))


def int_field(event: JsonDict, name: str, default: int = 0) -> int:
    """Read an optional integer field (bool excluded, float truncated when integral)."""
    raw = event.get(name)
    if isinstance(raw, bool):
        return default
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float) and raw.is_integer():
        return int(raw)
    return default


# Request/response documents carry heterogeneous shapes (lists of tool dicts,
# nested chat messages, usage blocks). They are typed as DocDict/DocValue so
# strict mypy accepts list/dict payloads without Any.
DocValue = object
DocDict = dict[str, object]
