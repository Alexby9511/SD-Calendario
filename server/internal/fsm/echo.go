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
)

// Command is the JSON payload accepted by Echo.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
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

// Apply implements raft.FSM.
func (e *Echo) Apply(log *raft.Log) any {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return Result{Err: fmt.Errorf("decode command: %w", err)}
	}
	return Result{Err: e.apply(cmd)}
}

func (e *Echo) apply(cmd Command) error {
	if cmd.Key == "" {
		return fmt.Errorf("empty key")
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
		return fmt.Errorf("unknown op %q", cmd.Op)
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

func (r Result) Error() string {
	if r.Err == nil {
		return ""
	}
	return r.Err.Error()
}

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
