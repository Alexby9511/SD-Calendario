// Package api exposes the node over HTTP.
//
// hashicorp/raft only applies commands on the leader, so any write that lands
// on a follower is forwarded internally. Without this the client would see
// spurious 503s on two thirds of its requests.
package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/hashicorp/raft"

	"github.com/Alexby9511/SD-Calendario/pkg/protocol"
	"github.com/Alexby9511/SD-Calendario/server/internal/raftnode"
)

const (
	maxBodyBytes   = 1 << 20
	applyTimeout   = 5 * time.Second
	forwardedByHdr = "X-Sdcal-Forwarded-By"
)

// Node is the subset of *raftnode.Node the HTTP layer needs. Tests substitute a
// fake to reach every branch without starting a cluster.
type Node interface {
	IsLeader() bool
	LeaderWithID() (string, string)
	State() string
	AppliedIndex() uint64
	LastIndex() uint64
	CommitIndex() uint64
	Apply(data []byte, timeout time.Duration) (uint64, any, error)
}

// ValidateFunc checks stateless command rules. It runs before the command is
// proposed, so a malformed command never consumes a raft index and every node
// (leader or follower) rejects it with the same answer.
type ValidateFunc func([]byte) error

// Config describes the HTTP layer.
type Config struct {
	Node     Node
	NodeID   string
	HTTPAddr string
	Peers    []raftnode.Peer
	// Lookup reads from the local FSM. It is advisory: a follower may be
	// slightly behind, which the real API will expose as read_index.
	Lookup func(key string) (string, bool)
	// Validate is optional; when nil, admission only checks that the body is
	// JSON. The calendar FSM always provides one.
	Validate ValidateFunc
	Logger   *slog.Logger
}

// Server serves the node API.
type Server struct {
	node     Node
	self     raftnode.Peer
	peers    map[string]string // node id -> http address
	lookup   func(string) (string, bool)
	validate ValidateFunc
	client   *http.Client
	logger   *slog.Logger
}

// New builds the HTTP server.
func New(cfg Config) *Server {
	// Keyed by node id, not by address: raft resolves hostnames to IPs when it
	// advertises, so the leader address it reports is already resolved and
	// would never match the configured "n1:7000".
	peers := make(map[string]string, len(cfg.Peers))
	var self raftnode.Peer
	for _, p := range cfg.Peers {
		peers[p.ID] = p.HTTPAddr
		if p.ID == cfg.NodeID {
			self = p
		}
	}
	if self.ID == "" {
		self = raftnode.Peer{ID: cfg.NodeID, HTTPAddr: cfg.HTTPAddr}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		node:     cfg.Node,
		self:     self,
		peers:    peers,
		lookup:   cfg.Lookup,
		validate: cfg.Validate,
		client:   &http.Client{Timeout: 10 * time.Second},
		logger:   logger,
	}
}

// Handler returns the routing table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("POST /v1/command", s.handleCommand)
	mux.HandleFunc("GET /v1/kv/{key}", s.handleKV)
	return mux
}

type statusResponse struct {
	ID           string `json:"id"`
	State        string `json:"state"`
	LeaderID     string `json:"leader_id"`
	LeaderRaft   string `json:"leader_raft_addr"`
	HTTPAddr     string `json:"http_addr"`
	AppliedIndex uint64 `json:"applied_index"`
	LastIndex    uint64 `json:"last_index"`
	CommitIndex  uint64 `json:"commit_index"`
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	addr, id := s.node.LeaderWithID()
	writeJSON(w, http.StatusOK, statusResponse{
		ID:           s.self.ID,
		State:        s.node.State(),
		LeaderID:     id,
		LeaderRaft:   addr,
		HTTPAddr:     s.self.HTTPAddr,
		AppliedIndex: s.node.AppliedIndex(),
		LastIndex:    s.node.LastIndex(),
		CommitIndex:  s.node.CommitIndex(),
	})
}

type commandResponse struct {
	Status   string `json:"status"`
	LogIndex uint64 `json:"log_index"`
}

func (s *Server) handleCommand(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "body too large")
		return
	}
	body = bytes.TrimSpace(body)
	if len(body) == 0 || !json.Valid(body) {
		writeError(w, http.StatusUnprocessableEntity, "invalid json")
		return
	}

	// Admission before replication: a command that cannot be applied is
	// rejected here, on whichever node received it, without a log entry.
	if s.validate != nil {
		if err := s.validate(body); err != nil {
			writeCodedError(w, err)
			return
		}
	}

	if !s.node.IsLeader() {
		if r.Header.Get(forwardedByHdr) != "" {
			// Already forwarded once and still not the leader: a loop means
			// the peer table is stale. Fail instead of bouncing forever.
			writeError(w, http.StatusServiceUnavailable, "not leader after forward")
			return
		}
		s.forward(w, r, body)
		return
	}

	index, response, err := s.node.Apply(body, applyTimeout)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			s.forward(w, r, body)
			return
		}
		s.logger.Error("apply failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "apply failed")
		return
	}

	// The entry is committed, but "committed" is not "applied successfully":
	// the FSM may reject it (state-dependent rules in the calendar FSM). The
	// client must see that rejection, not a 200.
	if appliedErr := applyError(response); appliedErr != nil {
		writeCodedError(w, appliedErr)
		return
	}

	writeJSON(w, http.StatusOK, commandResponse{Status: "committed", LogIndex: index})
}

// applyError extracts the rejection from an FSM response. ApplyError() is used
// instead of the error interface so that a zero-valued result does not look
// like a non-nil error.
func applyError(response any) error {
	type applyError interface{ ApplyError() error }
	if result, ok := response.(applyError); ok {
		return result.ApplyError()
	}
	if err, ok := response.(error); ok {
		return err
	}
	return nil
}

func (s *Server) forward(w http.ResponseWriter, r *http.Request, body []byte) {
	addr, id := s.node.LeaderWithID()
	if addr == "" || id == "" {
		writeError(w, http.StatusServiceUnavailable, "no leader")
		return
	}
	target, ok := s.peers[id]
	if !ok {
		s.logger.Warn("leader missing from peer table", "leader_addr", addr, "leader_id", id)
		writeError(w, http.StatusServiceUnavailable, "leader address unknown")
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, "http://"+target+"/v1/command", bytes.NewReader(body))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "cannot build forward request")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(forwardedByHdr, s.self.ID)

	resp, err := s.client.Do(req)
	if err != nil {
		s.logger.Warn("forward failed", "leader", target, "error", err)
		writeError(w, http.StatusServiceUnavailable, "forward failed")
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusUnprocessableEntity, "empty key")
		return
	}
	if s.lookup == nil {
		writeError(w, http.StatusServiceUnavailable, "lookup unavailable")
		return
	}
	value, ok := s.lookup(key)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": value})
}

type errorResponse struct {
	Error string `json:"error"`
}

// writeCodedError answers with the status carried by the error. Command
// rejections implement StatusCode; anything else is a 422 because the only
// errors that reach here come from admission or from the FSM.
func writeCodedError(w http.ResponseWriter, err error) {
	code := protocol.Unprocessable
	var coded interface{ StatusCode() protocol.Code }
	if errors.As(err, &coded) {
		code = coded.StatusCode()
	}
	writeError(w, code.HTTPStatus(), err.Error())
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
