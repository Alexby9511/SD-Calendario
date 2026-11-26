from dataclasses import dataclass, asdict
from hashlib import sha256
from typing import Any, Dict, Mapping, Optional

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from .canonical import canonical_bytes

VALID_TYPES = {
    "REQUEST", "REPLY", "COUNTER", "DECLINECOUNTER", "CANCEL",
    "CLOSE_POLL", "REOPEN", "REANCHOR", "BUSY_UPDATE", "SHARE_CALENDAR", "EVALUATE",
}

# Identity used by the leader ticker for internal expiration commands. Clients
# must never be able to register it or submit EVALUATE commands: both are
# rejected by the API and the state machine.
SYSTEM_USER_ID = "system"
RESERVED_USER_IDS = frozenset({SYSTEM_USER_ID})

MAX_SEQUENCE = 2**63 - 1


def _sequence_value(data: Mapping[str, Any], name: str) -> int:
    """Return a strict non-negative integer for a sequence field.

    ``int()`` is deliberately not used: it silently truncates ``1.9`` to ``1``,
    turns booleans into ``0``/``1`` and accepts numeric strings, so the signed
    value could differ from the received one. It also raised ``OverflowError``
    for JSON numbers such as ``1e999`` (parsed as ``inf``), an exception the
    state-machine adapter does not expect.
    """
    value = data[name]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"protocol: {name} must be an integer")
    if value < 0 or value > MAX_SEQUENCE:
        raise ValueError(f"protocol: {name} is out of range")
    return value


@dataclass
class Envelope:
    deployment_id: str
    group_id: str
    msg_id: str
    type: str
    uid: str
    sender_id: str
    key_id: str
    sequence: int
    seen_sequence: int
    client_sent_at: str
    payload: Dict[str, Any]
    signature: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Envelope":
        return cls(
            deployment_id=data["deployment_id"],
            group_id=data["group_id"],
            msg_id=data["msg_id"],
            type=data["type"],
            uid=data["uid"],
            sender_id=data["sender_id"],
            key_id=data.get("key_id", ""),
            sequence=_sequence_value(data, "sequence"),
            seen_sequence=_sequence_value(data, "seen_sequence"),
            client_sent_at=data["client_sent_at"],
            payload=data["payload"],
            signature=data.get("signature"),
        )

    def signing_input(self) -> bytes:
        fields = {
            "deployment_id": self.deployment_id,
            "group_id": self.group_id,
            "msg_id": self.msg_id,
            "type": self.type,
            "uid": self.uid,
            "sender_id": self.sender_id,
            "key_id": self.key_id,
            "sequence": self.sequence,
            "seen_sequence": self.seen_sequence,
            "client_sent_at": self.client_sent_at,
            "payload": self.payload,
        }
        return canonical_bytes(fields)

    def sign(self, private_key: Ed25519PrivateKey) -> None:
        if self.type == "EVALUATE":
            raise ValueError("protocol: system commands are not signed by clients")
        self.signature = private_key.sign(self.signing_input()).hex()

    def verify(self, public_key: Ed25519PublicKey) -> bool:
        if not self.signature:
            return False
        try:
            public_key.verify(bytes.fromhex(self.signature), self.signing_input())
            return True
        except (InvalidSignature, ValueError):
            return False

    def validate(self) -> None:
        for name in ("deployment_id", "group_id", "msg_id", "uid", "sender_id"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"protocol: empty {name}")
        if self.type not in VALID_TYPES:
            raise ValueError(f"protocol: unknown type {self.type!r}")
        if self.type != "EVALUATE" and not self.key_id.strip():
            raise ValueError("protocol: empty key_id")
        if not isinstance(self.payload, dict):
            raise ValueError("protocol: payload must be an object")
        if self.type == "REQUEST" and self.sequence == 1:
            if not self.uid.startswith(self.sender_id + ":"):
                raise ValueError("protocol: uid outside sender namespace")
        canonical_bytes(self.payload)

    def message_hash(self) -> str:
        return sha256(self.signing_input()).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
