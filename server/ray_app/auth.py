"""Bearer-token identity for calendar API requests."""

import hashlib
import json
import os
import secrets
from typing import Dict, Iterable


class AuthenticationNotConfigured(RuntimeError):
    """Raised when the API has no configured user tokens."""


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
