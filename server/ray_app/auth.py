"""Account credentials and bearer-token identity for calendar API requests."""

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from typing import Dict, Iterable

from pkg.protocol.envelope import RESERVED_USER_IDS

USERNAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.-]{2,31}$")
PASSWORD_ITERATIONS = 310_000
TOKEN_LIFETIME_SECONDS = 30 * 24 * 60 * 60

# Used to spend the same PBKDF2 work when the account does not exist, so login
# timing does not reveal which user IDs are registered.
_DUMMY_ACCOUNT = {
    "password_salt": "00" * 16,
    "password_hash": "00" * 32,
}


class AuthenticationNotConfigured(RuntimeError):
    """Raised when the API has no configured user tokens."""


class TokenSigner:
    """Issue and verify expiring bearer tokens using a shared server secret."""

    def __init__(self, secret: str) -> None:
        if len(secret.encode("utf-8")) < 32:
            raise ValueError("auth: SDCAL_AUTH_SECRET must be at least 32 bytes")
        self._secret = secret.encode("utf-8")

    @classmethod
    def from_environment(cls) -> "TokenSigner":
        secret = os.environ.get("SDCAL_AUTH_SECRET", "")
        if not secret:
            raise AuthenticationNotConfigured(
                "auth: configure SDCAL_AUTH_SECRET before serving calendar data"
            )
        return cls(secret)

    def issue(self, user_id: str) -> str:
        payload = json.dumps(
            {"sub": user_id, "exp": int(time.time()) + TOKEN_LIFETIME_SECONDS},
            separators=(",", ":"),
        ).encode("utf-8")
        encoded = base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")
        signature = hmac.new(
            self._secret, encoded.encode("ascii"), hashlib.sha256
        ).digest()
        return f"{encoded}.{base64.urlsafe_b64encode(signature).rstrip(b'=').decode('ascii')}"

    def authenticate(self, token: str) -> str:
        try:
            encoded, encoded_signature = token.split(".", 1)
            signature = _decode_base64(encoded_signature)
            payload = json.loads(_decode_base64(encoded))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("auth: invalid bearer token") from exc

        expected = hmac.new(
            self._secret, encoded.encode("ascii"), hashlib.sha256
        ).digest()
        if not hmac.compare_digest(signature, expected):
            raise ValueError("auth: invalid bearer token")
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get("sub"), str)
            or not isinstance(payload.get("exp"), int)
            or payload["exp"] <= int(time.time())
        ):
            raise ValueError("auth: invalid bearer token")
        return payload["sub"]


def _decode_base64(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def normalize_user_id(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("invalid_user_id")
    normalized = value.strip().lower()
    if normalized in RESERVED_USER_IDS:
        raise ValueError("invalid_user_id")
    if not USERNAME_PATTERN.fullmatch(normalized):
        raise ValueError("invalid_user_id")
    return normalized


def hash_password(password: object) -> Dict[str, str]:
    if not isinstance(password, str) or not 8 <= len(password) <= 256:
        raise ValueError("weak_password")
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, PASSWORD_ITERATIONS
    )
    return {
        "password_salt": salt.hex(),
        "password_hash": digest.hex(),
    }


def verify_password(password: object, account: Dict[str, str]) -> bool:
    if account is None:
        account = _DUMMY_ACCOUNT
    if not isinstance(password, str):
        return False
    try:
        salt = bytes.fromhex(account["password_salt"])
        expected = bytes.fromhex(account["password_hash"])
    except (KeyError, ValueError):
        return False
    actual = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, PASSWORD_ITERATIONS
    )
    return hmac.compare_digest(actual, expected)


class TokenRegistry:
    """Resolve bearer tokens to calendar user IDs without retaining raw tokens."""

    def __init__(self, users_to_tokens: Dict[str, Iterable[str]]) -> None:
        self._tokens: Dict[str, str] = {}
        for user_id, tokens in users_to_tokens.items():
            if not isinstance(user_id, str) or not user_id.strip():
                raise ValueError("auth: user IDs must be non-empty strings")
            for token in tokens:
                if not isinstance(token, str) or len(token) < 32:
                    raise ValueError("auth: bearer tokens must be at least 32 characters")
                digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
                if digest in self._tokens and self._tokens[digest] != user_id:
                    raise ValueError("auth: a token cannot belong to multiple users")
                self._tokens[digest] = user_id

    @classmethod
    def from_environment(cls) -> "TokenRegistry":
        raw = os.environ.get("SDCAL_API_TOKENS", "")
        if not raw:
            raise AuthenticationNotConfigured(
                "auth: configure SDCAL_API_TOKENS before serving calendar data"
            )
        try:
            configured = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("auth: SDCAL_API_TOKENS must be valid JSON") from exc
        if not isinstance(configured, dict) or not configured:
            raise ValueError("auth: SDCAL_API_TOKENS must map users to tokens")

        users_to_tokens: Dict[str, Iterable[str]] = {}
        for user_id, tokens in configured.items():
            if isinstance(tokens, str):
                users_to_tokens[user_id] = [tokens]
            elif isinstance(tokens, list):
                users_to_tokens[user_id] = tokens
            else:
                raise ValueError(
                    "auth: each user must map to a token or a list of tokens"
                )
        return cls(users_to_tokens)

    def authenticate(self, token: str) -> str:
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        for configured_digest, user_id in self._tokens.items():
            if secrets.compare_digest(digest, configured_digest):
                return user_id
        raise ValueError("auth: invalid bearer token")
