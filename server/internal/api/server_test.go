// server_test.go verifies the HTTP API against fake and in-memory nodes.
// It covers admission, forwarding, status, and key/value read behavior.
package api_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/Alexby9511/SD-Calendario/pkg/protocol"
	"github.com/Alexby9511/SD-Calendario/server/internal/api"
	"github.com/Alexby9511/SD-Calendario/server/internal/fsm"
	"github.com/Alexby9511/SD-Calendario/server/internal/raftnode"
)

// fakeNode lets the HTTP layer be tested without a cluster: every branch of
// handleCommand depends on state that is trivial to set here and slow to force
// in a real election.
type fakeNode struct {
	mu sync.Mutex

	leader     bool
	leaderID   string
	leaderAddr string
	state      string

	appliedIndex uint64
	lastIndex    uint64
	commitIndex  uint64

	applyIndex    uint64
	applyResponse any
	applyErr      error
	applyCalls    int
	lastBody      []byte
}

// IsLeader reports whether the fake node is marked as leader.
func (f *fakeNode) IsLeader() bool { return f.leader }

// LeaderWithID returns the fake leader address and ID.
func (f *fakeNode) LeaderWithID() (string, string) { return f.leaderAddr, f.leaderID }

// State returns the configured fake Raft role.
func (f *fakeNode) State() string {
	if f.state == "" {
		return "Follower"
	}
	return f.state
}

// AppliedIndex returns the fake applied index.
func (f *fakeNode) AppliedIndex() uint64 { return f.appliedIndex }
// LastIndex returns the fake last log index.
func (f *fakeNode) LastIndex() uint64 { return f.lastIndex }
// CommitIndex returns the fake committed log index.
func (f *fakeNode) CommitIndex() uint64 { return f.commitIndex }

// Apply records a command and returns the configured test result.
func (f *fakeNode) Apply(data []byte, _ time.Duration) (uint64, any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applyCalls++
	f.lastBody = append([]byte(nil), data...)
	return f.applyIndex, f.applyResponse, f.applyErr
}

// calls returns the number of commands applied to the fake node.
func (f *fakeNode) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applyCalls
}

// quietLogger creates a logger that discards test output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newServer creates an HTTP test server around the supplied node.
func newServer(node api.Node, peers []raftnode.Peer, validate api.ValidateFunc) *httptest.Server {
	srv := api.New(api.Config{
		Node:     node,
		NodeID:   "n1",
		HTTPAddr: "n1:8080",
		Peers:    peers,
		Validate: validate,
		Logger:   quietLogger(),
	})
	return httptest.NewServer(srv.Handler())
}

// postCommand sends a JSON command to the test API.
func postCommand(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url+"/v1/command", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// decodeError reads and returns the API error message from a response.
func decodeError(t *testing.T, resp *http.Response) string {
	t.Helper()
	var body struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return body.Error
}

// TestCommandRejectsInvalidJSONWithoutApplying verifies malformed JSON reaches neither validation nor Raft.
func TestCommandRejectsInvalidJSONWithoutApplying(t *testing.T) {
	node := &fakeNode{leader: true}
	srv := newServer(node, nil, (&fakeMachine{}).Validate)
	defer srv.Close()

	resp := postCommand(t, srv.URL, `{"op":`)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", resp.StatusCode)
	}
	if node.calls() != 0 {
		t.Fatal("invalid JSON reached Apply")
	}
}

// TestCommandRejectsOversizedBody verifies requests beyond the body limit are rejected.
func TestCommandRejectsOversizedBody(t *testing.T) {
	node := &fakeNode{leader: true}
	srv := newServer(node, nil, nil)
	defer srv.Close()

	resp := postCommand(t, srv.URL, `{"op":"set","key":"a","value":"`+strings.Repeat("x", 2<<20)+`"}`)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
	if node.calls() != 0 {
		t.Fatal("oversized body reached Apply")
	}
}

// Admission is the half of H1 that keeps invalid commands out of the log.
// TestAdmissionRejectsBeforeApply verifies invalid domain commands consume no Raft index.
func TestAdmissionRejectsBeforeApply(t *testing.T) {
	node := &fakeNode{leader: true}
	srv := newServer(node, nil, fsm.New().Validate)
	defer srv.Close()

	resp := postCommand(t, srv.URL, `{"op":"explode","key":"a"}`)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", resp.StatusCode)
	}
	if got := decodeError(t, resp); !strings.Contains(got, "unknown op") {
		t.Fatalf("error = %q, want the rejection reason", got)
	}
	if node.calls() != 0 {
		t.Fatal("an invalid command consumed a raft index")
	}
}

// H1 regression: a rejection returned by the FSM used to be discarded, so the
// client saw 200 committed for a command that was never applied.
// TestCommandPropagatesFSMRejection verifies FSM rejection codes reach the HTTP caller.
func TestCommandPropagatesFSMRejection(t *testing.T) {
	node := &fakeNode{
		leader:        true,
		applyResponse: fsm.Result{Err: &fsm.CommandError{Code: protocol.Conflict, Reason: "stale sequence"}},
	}
	srv := newServer(node, nil, nil)
	defer srv.Close()

	resp := postCommand(t, srv.URL, `{"op":"set","key":"a","value":"1"}`)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
	if got := decodeError(t, resp); got != "stale sequence" {
		t.Fatalf("error = %q", got)
	}
}

// TestCommandSuccessReportsLogIndex verifies successful commits expose their Raft index.
func TestCommandSuccessReportsLogIndex(t *testing.T) {
	node := &fakeNode{leader: true, applyIndex: 42, applyResponse: fsm.Result{}}
	srv := newServer(node, nil, nil)
	defer srv.Close()

	resp := postCommand(t, srv.URL, `{"op":"set","key":"a","value":"1"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Status   string `json:"status"`
		LogIndex uint64 `json:"log_index"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "committed" || body.LogIndex != 42 {
		t.Fatalf("body = %+v", body)
	}
}

// TestFollowerForwardsToLeader verifies follower writes are forwarded with loop-protection headers.
func TestFollowerForwardsToLeader(t *testing.T) {
	var (
		mu      sync.Mutex
		gotBody string
		gotHdr  string
	)
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotBody = string(raw)
		gotHdr = r.Header.Get("X-Sdcal-Forwarded-By")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"committed","log_index":7}`))
	}))
	defer leader.Close()

	peers := []raftnode.Peer{
		{ID: "n1", RaftAddr: "n1", HTTPAddr: strings.TrimPrefix(leader.URL, "http://")},
		{ID: "n2", RaftAddr: "n2", HTTPAddr: "n2:8080"},
	}
	node := &fakeNode{leader: false, leaderID: "n1", leaderAddr: "10.0.0.1:7000"}
	srv := newServer(node, peers, fsm.New().Validate)
	defer srv.Close()

	resp := postCommand(t, srv.URL, `{"op":"set","key":"a","value":"1"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 from the leader", resp.StatusCode)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotBody != `{"op":"set","key":"a","value":"1"}` {
		t.Fatalf("leader received %q", gotBody)
	}
	if gotHdr != "n1" {
		t.Fatalf("X-Sdcal-Forwarded-By = %q, want n1", gotHdr)
	}
	if node.calls() != 0 {
		t.Fatal("follower applied the command locally")
	}
}

// A follower rejects malformed commands itself instead of forwarding garbage.
// TestFollowerRejectsInvalidCommandWithoutForwarding verifies invalid writes stay on the receiving follower.
func TestFollowerRejectsInvalidCommandWithoutForwarding(t *testing.T) {
	var forwarded bool
	leader := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		forwarded = true
	}))
	defer leader.Close()

	peers := []raftnode.Peer{
		{ID: "n1", RaftAddr: "n1", HTTPAddr: strings.TrimPrefix(leader.URL, "http://")},
		{ID: "n2", RaftAddr: "n2", HTTPAddr: "n2:8080"},
	}
	node := &fakeNode{leader: false, leaderID: "n1", leaderAddr: "10.0.0.1:7000"}
	srv := newServer(node, peers, fsm.New().Validate)
	defer srv.Close()

	resp := postCommand(t, srv.URL, `{"op":"explode","key":"a"}`)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", resp.StatusCode)
	}
	if forwarded {
		t.Fatal("invalid command was forwarded")
	}
}

// TestForwardLoopGuard verifies already-forwarded requests stop instead of looping.
func TestForwardLoopGuard(t *testing.T) {
	node := &fakeNode{leader: false, leaderID: "n1", leaderAddr: "10.0.0.1:7000"}
	srv := newServer(node, []raftnode.Peer{{ID: "n1", HTTPAddr: "127.0.0.1:1"}}, nil)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/command", strings.NewReader(`{"op":"set","key":"a"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Sdcal-Forwarded-By", "n3")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if got := decodeError(t, resp); got != "not leader after forward" {
		t.Fatalf("error = %q", got)
	}
}

// TestForwardUnknownLeader verifies a missing leader peer produces a service-unavailable response.
func TestForwardUnknownLeader(t *testing.T) {
	node := &fakeNode{leader: false, leaderID: "n9", leaderAddr: "10.0.0.9:7000"}
	srv := newServer(node, []raftnode.Peer{{ID: "n1", HTTPAddr: "127.0.0.1:1"}}, nil)
	defer srv.Close()

	resp := postCommand(t, srv.URL, `{"op":"set","key":"a"}`)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if got := decodeError(t, resp); got != "leader address unknown" {
		t.Fatalf("error = %q", got)
	}
}

// TestForwardNoLeader verifies writes fail clearly while Raft has no known leader.
func TestForwardNoLeader(t *testing.T) {
	node := &fakeNode{leader: false}
	srv := newServer(node, nil, nil)
	defer srv.Close()

	resp := postCommand(t, srv.URL, `{"op":"set","key":"a"}`)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if got := decodeError(t, resp); got != "no leader" {
		t.Fatalf("error = %q", got)
	}
}

// The leader can be lost between IsLeader() and Apply(); raft reports it and
// the request must be retried against the new leader, not failed with 503.
// TestLeadershipLostWhileApplyingForwards verifies leadership loss retries through the leader.
func TestLeadershipLostWhileApplyingForwards(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"committed","log_index":9}`))
	}))
	defer leader.Close()

	peers := []raftnode.Peer{{ID: "n1", HTTPAddr: strings.TrimPrefix(leader.URL, "http://")}}
	node := &fakeNode{
		leader:     true,
		leaderID:   "n1",
		leaderAddr: "10.0.0.1:7000",
		applyErr:   raft.ErrNotLeader,
	}
	srv := newServer(node, peers, nil)
	defer srv.Close()

	resp := postCommand(t, srv.URL, `{"op":"set","key":"a"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 after forwarding", resp.StatusCode)
	}
	if node.calls() != 1 {
		t.Fatalf("apply calls = %d, want 1", node.calls())
	}
}

// TestStatusReportsClusterView verifies status exposes node, leader, and Raft index state.
func TestStatusReportsClusterView(t *testing.T) {
	node := &fakeNode{
		leader:       true,
		leaderID:     "n1",
		leaderAddr:   "10.0.0.1:7000",
		state:        "Leader",
		appliedIndex: 11,
		lastIndex:    12,
		commitIndex:  11,
	}
	srv := newServer(node, nil, nil)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["id"] != "n1" || body["state"] != "Leader" || body["leader_id"] != "n1" {
		t.Fatalf("body = %v", body)
	}
	if body["applied_index"].(float64) != 11 || body["commit_index"].(float64) != 11 {
		t.Fatalf("indices = %v", body)
	}
}

// TestKV verifies existing and missing key lookups return the expected HTTP responses.
func TestKV(t *testing.T) {
	values := map[string]string{"a": "1"}
	srv := api.New(api.Config{
		Node:     &fakeNode{},
		NodeID:   "n1",
		HTTPAddr: "n1:8080",
		Lookup: func(key string) (string, bool) {
			v, ok := values[key]
			return v, ok
		},
		Logger: quietLogger(),
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/kv/a")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	resp2, err := http.Get(ts.URL + "/v1/kv/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp2.StatusCode)
	}
}

// fakeMachine provides a validator without a real FSM when a test only cares
// that admission ran.
type fakeMachine struct{}

// Validate accepts any test payload without applying domain rules.
func (fakeMachine) Validate([]byte) error { return nil }
