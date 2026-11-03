package protocol

import (
	"crypto/sha256"
	"fmt"
	"strings"

	jsoncanonicalizer "github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
)

// ulidAlphabet is Crockford base32, used by ULIDs.
const ulidAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// Canonicalize returns the RFC 8785 (JCS) serialization of b.
//
// JCS is required because the signature must not depend on key order or
// whitespace. The same JSON produced by Go and by any other implementation
// yields the same bytes.
func Canonicalize(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("canonical: empty document")
	}
	out, err := jsoncanonicalizer.Transform(b)
	if err != nil {
		return nil, fmt.Errorf("canonical: %w", err)
	}
	return out, nil
}

// IsULID reports whether s is a syntactically valid, uppercase ULID.
func IsULID(s string) bool {
	if len(s) != 26 {
		return false
	}
	// 128 bits do not fit in 26 * 5 = 130, so the first character is limited.
	if s[0] > '7' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !strings.ContainsRune(ulidAlphabet, rune(s[i])) {
			return false
		}
	}
	return true
}

func sha256Sum(b []byte) [32]byte { return sha256.Sum256(b) }
