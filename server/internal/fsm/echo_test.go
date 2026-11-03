package fsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/Alexby9511/SD-Calendario/pkg/protocol"
)

func applyCommands(t *testing.T, e *Echo, cmds ...Command) {
	t.Helper()
	for _, cmd := range cmds {
		raw, err := json.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		res, ok := e.Apply(&raft.Log{Data: raw}).(Result)
		if !ok {
			t.Fatalf("unexpected result type %T", res)
		}
		if res.Err != nil {
			t.Fatalf("apply %v: %v", cmd, res.Err)
		}
	}
}

func TestApplyIsDeterministic(t *testing.T) {
	cmds := []Command{
		{Op: "set", Key: "a", Value: "1"},
		{Op: "set", Key: "b", Value: "2"},
		{Op: "set", Key: "a", Value: "3"},
		{Op: "delete", Key: "b"},
	}

	first := New()
	second := New()
	applyCommands(t, first, cmds...)
	applyCommands(t, second, cmds...)

	for _, key := range []string{"a", "b", "c"} {
		av, aok := first.Get(key)
		bv, bok := second.Get(key)
		if aok != bok || av != bv {
			t.Fatalf("divergence on %q: (%q,%v) vs (%q,%v)", key, av, aok, bv, bok)
		}
	}
}

func TestApplyRejectsUnknownOp(t *testing.T) {
	e := New()
	raw, _ := json.Marshal(Command{Op: "explode", Key: "a"})
	res := e.Apply(&raft.Log{Data: raw}).(Result)
	if res.Err == nil {
		t.Fatal("unknown op accepted")
	}
}

func TestApplyRejectsEmptyKey(t *testing.T) {
	e := New()
	raw, _ := json.Marshal(Command{Op: "set", Key: "", Value: "x"})
	res := e.Apply(&raft.Log{Data: raw}).(Result)
	if res.Err == nil {
		t.Fatal("empty key accepted")
	}
}

// Admission and apply must agree: whatever Validate rejects cannot be applied
// either, otherwise a command could slip into the log by another path.
func TestValidateMatchesApply(t *testing.T) {
	cases := [][]byte{
		[]byte(`{"op":"set","key":"a","value":"1"}`),
		[]byte(`{"op":"delete","key":"a"}`),
		[]byte(`{"op":"set","key":"","value":"1"}`),
		[]byte(`{"op":"explode","key":"a"}`),
		[]byte(`{"op":"set"`),
		[]byte(`[]`),
	}
	for _, raw := range cases {
		admissionErr := New().Validate(raw)
		applied := New().Apply(&raft.Log{Data: raw}).(Result).Err
		if (admissionErr == nil) != (applied == nil) {
			t.Errorf("input %s: admission err %v, apply err %v", raw, admissionErr, applied)
		}
	}
}

func TestCommandErrorCarriesStatus(t *testing.T) {
	raw := []byte(`{"op":"explode","key":"a"}`)
	err := New().Validate(raw)
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("want *CommandError, got %T", err)
	}
	if cmdErr.StatusCode() != protocol.Unprocessable {
		t.Fatalf("status = %v, want %v", cmdErr.StatusCode(), protocol.Unprocessable)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	original := New()
	applyCommands(t, original,
		Command{Op: "set", Key: "a", Value: "1"},
		Command{Op: "set", Key: "b", Value: "2"},
	)

	snap, err := original.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	restored := New()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.buf.Bytes()))); err != nil {
		t.Fatal(err)
	}

	if restored.Len() != original.Len() {
		t.Fatalf("len %d != %d", restored.Len(), original.Len())
	}
	for _, key := range []string{"a", "b"} {
		want, _ := original.Get(key)
		got, ok := restored.Get(key)
		if !ok || got != want {
			t.Fatalf("key %q: got (%q,%v), want %q", key, got, ok, want)
		}
	}
}

// A snapshot must be a point-in-time copy: later applies to the live machine
// cannot leak into an already taken snapshot.
func TestSnapshotIsIsolated(t *testing.T) {
	e := New()
	applyCommands(t, e, Command{Op: "set", Key: "a", Value: "1"})

	snap, err := e.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	applyCommands(t, e, Command{Op: "set", Key: "b", Value: "2"})

	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	restored := New()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.buf.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if _, ok := restored.Get("b"); ok {
		t.Fatal("snapshot observed a write made after it was taken")
	}
}

type memSink struct {
	buf       bytes.Buffer
	cancelled bool
}

func (s *memSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *memSink) Close() error                { return nil }
func (s *memSink) ID() string                  { return "mem" }
func (s *memSink) Cancel() error               { s.cancelled = true; return nil }
