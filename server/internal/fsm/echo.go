// Package fsm contains the replicated state machine.
//
// Milestone 1 uses a trivial key/value echo machine: its only job is to prove
// that the cluster converges, survives a node loss and can be replayed from a
// snapshot. The calendar FSM replaces it in milestone 2 behind the same
// raft.FSM interface.
package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"

	"github.com/Alexby9511/SD-Calendario/pkg/protocol"
)

// Command is the JSON payload accepted by Echo.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// CommandError is a rejection caused by the command itself, as opposed to an
// infrastructure failure. It carries the protocol code so the HTTP layer can
// answer 422/409/... without importing the FSM.
type CommandError struct {
	Code   protocol.Code
	Reason string
}

func (e *CommandError) Error() string { return e.Reason }

// StatusCode implements the interface read by the API layer.
func (e *CommandError) StatusCode() protocol.Code { return e.Code }

func invalid(format string, args ...any) error {
	return &CommandError{Code: protocol.Unprocessable, Reason: fmt.Sprintf(format, args...)}
}

// DecodeCommand parses and validates a command body.
//
// This is the single definition of what a valid command is: admission calls it
// before proposing to the log, and Apply calls it again as the authoritative
// check. Sharing the function is what keeps the two from drifting apart.
func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return Command{}, invalid("decode command: %v", err)
	}
	if err := cmd.validate(); err != nil {
		return Command{}, err
	}
	return cmd, nil
}

func (c Command) validate() error {
	if c.Key == "" {
		return invalid("empty key")
	}
	switch c.Op {
	case "set", "delete":
	default:
		return invalid("unknown op %q", c.Op)
	}
	return nil
}

// Echo is a deterministic map guarded by a mutex. raft serializes Apply calls,
// but reads come from HTTP handlers, so the lock is still required.
type Echo struct {
	mu   sync.RWMutex
	data map[string]string
}

// New returns an empty Echo machine.
func New() *Echo {
	return &Echo{data: make(map[string]string)}
}

// Validate checks a command without executing it. The HTTP layer runs it before
// the command reaches raft, so a malformed command never consumes a log index.
func (e *Echo) Validate(data []byte) error {
	_, err := DecodeCommand(data)
	return err
}

// Apply implements raft.FSM.
func (e *Echo) Apply(log *raft.Log) any {
	return Result{Err: e.apply(log.Data)}
}

func (e *Echo) apply(data []byte) error {
	cmd, err := DecodeCommand(data)
	if err != nil {
		return err
	}
	switch cmd.Op {
	case "set":
		e.mu.Lock()
		defer e.mu.Unlock()
		e.data[cmd.Key] = cmd.Value
	case "delete":
		e.mu.Lock()
		defer e.mu.Unlock()
		delete(e.data, cmd.Key)
	default:
		// Unreachable: DecodeCommand rejects unknown ops. Kept so adding an
		// op to the switch cannot silently fall through.
		return invalid("unknown op %q", cmd.Op)
	}
	return nil
}

// Get returns a copy of the value for key.
func (e *Echo) Get(key string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	v, ok := e.data[key]
	return v, ok
}

// Len returns the number of live keys.
func (e *Echo) Len() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.data)
}

// Snapshot implements raft.FSM. It copies the map so later applies cannot
// mutate the snapshot while it is being written.
func (e *Echo) Snapshot() (raft.FSMSnapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	data := make(map[string]string, len(e.data))
	for k, v := range e.data {
		data[k] = v
	}
	return &snapshot{data: data}, nil
}

// Restore implements raft.FSM.
func (e *Echo) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data = data
	return nil
}

// Result is the value returned by Apply and read by the HTTP layer.
type Result struct {
	Err error
}

// ApplyError exposes the outcome through a method instead of making Result
// implement error: an error interface holding a Result with Err == nil would be
// non-nil and look like a failure.
func (r Result) ApplyError() error { return r.Err }

type snapshot struct {
	data map[string]string
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	encoded, err := json.Marshal(s.data)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	if _, err := sink.Write(encoded); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
