from enum import IntEnum


class Code(IntEnum):
    OK = 200
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    CONFLICT = 409
    GONE = 410
    UNPROCESSABLE = 422
    TOO_MANY_REQUESTS = 429
    UNAVAILABLE = 503

    @property
    def is_success(self) -> bool:
        return self == Code.OK

    @property
    def is_retryable(self) -> bool:
        return self in (Code.UNAVAILABLE, Code.TOO_MANY_REQUESTS)

    @property
    def should_discard(self) -> bool:
        return self == Code.GONE

    @property
    def needs_refresh(self) -> bool:
        return self == Code.CONFLICT
