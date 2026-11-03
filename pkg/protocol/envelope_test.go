package protocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
)

const testULID = "01J9Z3K6Y7Q8R9S0T1V2W3X4Y5"

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

func TestValidateAllowsUpdatesOutsideNamespaceCheck(t *testing.T) {
	env := newEnvelope(t)
	env.Sequence = 2
	env.UID = "alice:" + testULID
	if err := env.Validate(); err != nil {
		t.Fatalf("valid update rejected: %v", err)
	}
}

func TestValidateRejectsMissingFields(t *testing.T) {
	env := newEnvelope(t)
	env.MsgID = ""
	if err := env.Validate(); err == nil {
		t.Fatal("accepted an envelope without msg_id")
	}
}

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
