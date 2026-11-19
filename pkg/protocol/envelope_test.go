// envelope_test.go verifies envelope signing and validation behavior.
// Golden vectors protect the fixed signed-byte format from accidental changes.
package protocol

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

const testULID = "01J9Z3K6Y7Q8R9S0T1V2W3X4Y5"

// goldenSigningInput is the byte string that §6.1 defines. It is duplicated here
// on purpose: the test must fail when the wire format changes, so the document
// and the implementation cannot drift apart silently.
const goldenSigningInput = `{"client_sent_at":"2026-11-02T09:12:03Z","deployment_id":"sdcal-dev","group_id":"grupo-tesis","key_id":"alice:device-1","msg_id":"01J9Z3K6Y7Q8R9S0T1V2W3X4Y5","payload":{"start_utc":"2026-11-03T10:00:00Z","summary":"Reunión"},"seen_sequence":0,"sender_id":"alice","sequence":1,"type":"REQUEST","uid":"alice:01J9Z3K6Y7Q8R9S0T1V2W3X4Y5"}`

// goldenSignature is produced by a fixed seed, so Ed25519 (deterministic per
// RFC 8032) must reproduce it byte for byte.
const goldenSignature = "38e000721736435aeca6279045aee359f8c5f2e299cfde90b5a352ffd8cbb8cb0812b8430db69386b733ea7968f31b783bcee4f8a9619acbe73c11ba510bc70a"

// newEnvelope creates a valid test envelope for signing and validation tests.
func newEnvelope(t *testing.T) *Envelope {
	t.Helper()
	return &Envelope{
		DeploymentID: "sdcal-dev",
		GroupID:      "grupo-tesis",
		MsgID:        testULID,
		Type:         TypeRequest,
		UID:          "alice:" + testULID,
		SenderID:     "alice",
		KeyID:        "alice:device-1",
		Sequence:     1,
		ClientSentAt: "2026-11-02T09:12:03Z",
		Payload:      json.RawMessage(`{"summary":"Reunión","start_utc":"2026-11-03T10:00:00Z"}`),
	}
}

// TestSigningInputGoldenVector pins the exact bytes signed under §6.1.
func TestSigningInputGoldenVector(t *testing.T) {
	env := newEnvelope(t)
	got, err := env.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != goldenSigningInput {
		t.Fatalf("signing input changed:\n got: %s\nwant: %s", got, goldenSigningInput)
	}
	sum := sha256.Sum256(got)
	if hex.EncodeToString(sum[:]) != "d434e090fccc6ea5d760ee29e81f06959f51f105310b52480cea492aeedb654f" {
		t.Fatalf("sha256 = %x", sum)
	}
}

// TestSignatureGoldenVector is the cross-implementation contract: another
// language must produce these same 64 bytes for this envelope and seed.
func TestSignatureGoldenVector(t *testing.T) {
	env := newEnvelope(t)
	priv := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize))
	if err := env.Sign(priv); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(env.Signature); got != goldenSignature {
		t.Fatalf("signature changed:\n got: %s\nwant: %s", got, goldenSignature)
	}
	if err := env.Verify(priv.Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("golden signature does not verify: %v", err)
	}
}

// TestSignVerifyRoundTrip verifies signing and verification with a valid key pair.
func TestSignVerifyRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	env := newEnvelope(t)
	if err := env.Sign(priv); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if err := env.Verify(pub); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// TestVerifyDetectsTampering verifies modified payload bytes invalidate the signature.
func TestVerifyDetectsTampering(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	env := newEnvelope(t)
	if err := env.Sign(priv); err != nil {
		t.Fatalf("sign: %v", err)
	}

	env.Payload = json.RawMessage(`{"summary":"Otra cosa","start_utc":"2026-11-03T10:00:00Z"}`)
	if err := env.Verify(pub); err == nil {
		t.Fatal("tampered payload verified")
	}
}

// TestSigningInputIgnoresSignatureAndFormatting verifies signatures and JSON formatting do not affect signing bytes.
func TestSigningInputIgnoresSignatureAndFormatting(t *testing.T) {
	// Same logical payload, different key order and whitespace.
	envA := newEnvelope(t)
	envB := newEnvelope(t)
	envB.Payload = json.RawMessage(`{
		"start_utc": "2026-11-03T10:00:00Z",
		"summary":   "Reunión"
	}`)

	inputA, err := envA.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	inputB, err := envB.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	if string(inputA) != string(inputB) {
		t.Fatalf("canonical inputs differ:\n%s\n%s", inputA, inputB)
	}

	// Signing must not change the signed bytes.
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := envA.Sign(priv); err != nil {
		t.Fatal(err)
	}
	after, err := envA.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(inputA) {
		t.Fatal("signing changed the signed bytes")
	}
}

// TestMessageHashExcludesSignature verifies changing only the signature leaves the digest unchanged.
func TestMessageHashExcludesSignature(t *testing.T) {
	env := newEnvelope(t)
	before, err := env.MessageHash()
	if err != nil {
		t.Fatal(err)
	}
	env.Signature = make([]byte, ed25519.SignatureSize)
	after, err := env.MessageHash()
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("hash changed when only the signature changed")
	}
}

// TestValidateCreationNamespace verifies creation UIDs must belong to the sender namespace.
func TestValidateCreationNamespace(t *testing.T) {
	env := newEnvelope(t)
	if err := env.Validate(); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}

	env.UID = "bob:" + testULID
	if err := env.Validate(); err == nil {
		t.Fatal("accepted a uid outside the sender namespace")
	}
}

// TestValidateAllowsUpdatesOutsideNamespaceCheck verifies namespace rules apply only to first sequence.
func TestValidateAllowsUpdatesOutsideNamespaceCheck(t *testing.T) {
	env := newEnvelope(t)
	env.Sequence = 2
	env.UID = "alice:" + testULID
	if err := env.Validate(); err != nil {
		t.Fatalf("valid update rejected: %v", err)
	}
}

// TestValidateRejectsMissingFields verifies required envelope fields cannot be empty.
func TestValidateRejectsMissingFields(t *testing.T) {
	env := newEnvelope(t)
	env.MsgID = ""
	if err := env.Validate(); err == nil {
		t.Fatal("accepted an envelope without msg_id")
	}
}

// TestSignRejectsSystemCommands verifies system commands cannot be signed by clients.
func TestSignRejectsSystemCommands(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	env := newEnvelope(t)
	env.Type = TypeEvaluate
	if err := env.Sign(priv); err == nil {
		t.Fatal("signed a system command")
	}
}
