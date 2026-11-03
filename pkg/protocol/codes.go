package protocol

// Code is the response status defined in docs/arquitectura.md §6.4.
type Code int

const (
	OK              Code = 200
	Unauthorized    Code = 401
	Forbidden       Code = 403
	Conflict        Code = 409
	Gone            Code = 410
	Unprocessable   Code = 422
	TooManyRequests Code = 429
	Unavailable     Code = 503
)

// IsSuccess reports whether the code means the command was applied.
func (c Code) IsSuccess() bool { return c == OK }

// HTTPStatus maps the protocol code to the status line used on the wire.
//
// Protocol codes are HTTP-shaped on purpose (RFC-style 4xx/5xx that a proxy
// understands without a translation table); the switch is explicit so adding a
// code cannot silently produce 200.
func (c Code) HTTPStatus() int {
	switch c {
	case OK, Unauthorized, Forbidden, Conflict, Gone, Unprocessable, TooManyRequests, Unavailable:
		return int(c)
	default:
		return 500
	}
}

// IsRetryable reports whether the client should send the same msg_id again.
func (c Code) IsRetryable() bool { return c == Unavailable || c == TooManyRequests }

// ShouldDiscard reports whether the outbox entry can be dropped: the server
// has taken a final decision about the message.
func (c Code) ShouldDiscard() bool { return c == Gone }

// NeedsRefresh reports whether the client must re-read state and re-emit.
func (c Code) NeedsRefresh() bool { return c == Conflict }

// String returns the canonical wire name.
func (c Code) String() string {
	switch c {
	case OK:
		return "ok"
	case Unauthorized:
		return "unauthorized"
	case Forbidden:
		return "forbidden"
	case Conflict:
		return "conflict"
	case Gone:
		return "gone"
	case Unprocessable:
		return "unprocessable"
	case TooManyRequests:
		return "rate_limited"
	case Unavailable:
		return "unavailable"
	default:
		return "unknown"
	}
}
