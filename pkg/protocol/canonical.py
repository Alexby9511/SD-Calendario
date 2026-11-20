import json
from typing import Any

ULID_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def canonicalize(value: Any) -> str:
    """Return a deterministic JSON representation."""
    canonical_bytes(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def canonical_bytes(value: Any) -> bytes:
    """Validate and serialize JSON as canonical bytes for signatures."""
    _reject_float(value)
    if isinstance(value, bytes):
        return bytes.fromhex(value.decode("ascii"))
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def _reject_float(value: Any) -> None:
    if isinstance(value, float):
        raise ValueError("protocol: floats are not allowed")
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("protocol: JSON object keys must be strings")
            _reject_float(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_float(item)


def is_ulid(value: str) -> bool:
    if not isinstance(value, str) or len(value) != 26:
        return False
    if value[0] > "7":
        return False
    return all(char in ULID_ALPHABET for char in value)
