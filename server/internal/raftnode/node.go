// Package raftnode wires hashicorp/raft to durable storage and a TCP
// transport. It knows nothing about calendars: the FSM is injected.
package raftnode

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// slogWriter adapts a slog.Logger to the io.Writer that parts of hashicorp/raft
// still ask for, so every line goes through the logger the node was configured
// with instead of straight to stderr.
type slogWriter struct{ logger *slog.Logger }

func (w slogWriter) Write(p []byte) (int, error) {
	w.logger.Warn(strings.TrimSpace(string(p)))
	return len(p), nil
}

// newRaftLogger returns the hclog adapter used by raft, its snapshot store and
// its transport. WARN is the useful level for an operator, which is also why
// raft's DEBUG default is never used here.
func newRaftLogger(logger *slog.Logger) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.Warn,
		Output: slogWriter{logger: logger},
	})
}

// Peer is a cluster member. RaftAddr is the address used by the consensus
// transport; HTTPAddr is used for forwarding writes to the leader.
type Peer struct {
	ID       string
	RaftAddr string
	HTTPAddr string
}

// Config describes a single node.
type Config struct {
	ID        string
	BindAddr  string // e.g. 0.0.0.0:7000
	RaftAddr  string // advertised, e.g. n1:7000
	DataDir   string
	Bootstrap bool
	Peers     []Peer
	FSM       raft.FSM
	Logger    *slog.Logger
	// Transport overrides the TCP transport. Tests use an in-memory one;
	// production leaves it nil.
	Transport raft.Transport
}

// Node owns the raft instance and its resources.
type Node struct {
	raft      *raft.Raft
	store     *raftboltdb.BoltStore
	transport raft.Transport
	closer    io.Closer
	logger    *slog.Logger
}

// New starts a node. Bootstrap only has an effect on an empty data directory:
// restarting a bootstrapped node must not re-bootstrap, or Raft would lose its
// configuration.
func New(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("raftnode: empty id")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("raftnode: empty data dir")
	}
	if cfg.FSM == nil {
		return nil, fmt.Errorf("raftnode: nil FSM")
	}
	if cfg.Transport == nil && (cfg.BindAddr == "" || cfg.RaftAddr == "") {
		return nil, fmt.Errorf("raftnode: empty addresses")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	raftLog := newRaftLogger(logger)

	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return nil, fmt.Errorf("raftnode: data dir: %w", err)
	}

	store, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("raftnode: bolt store: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStoreWithLogger(cfg.DataDir, 2, raftLog)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("raftnode: snapshot store: %w", err)
	}

	var (
		transport raft.Transport
		closer    io.Closer
	)
	if cfg.Transport != nil {
		transport = cfg.Transport
	} else {
		advertise, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("raftnode: resolve advertise address: %w", err)
		}
		tcpTransport, err := raft.NewTCPTransportWithLogger(cfg.BindAddr, advertise, 5, 10*time.Second, raftLog)
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("raftnode: transport: %w", err)
		}
		transport = tcpTransport
		closer = tcpTransport
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.ID)
	rc.Logger = raftLog
	rc.SnapshotInterval = 30 * time.Second
	rc.SnapshotThreshold = 512

	r, err := raft.NewRaft(rc, cfg.FSM, store, store, snapshots, transport)
	if err != nil {
		if closer != nil {
			_ = closer.Close()
		}
		_ = store.Close()
		return nil, fmt.Errorf("raftnode: new raft: %w", err)
	}

	n := &Node{raft: r, store: store, transport: transport, closer: closer, logger: logger}

	if cfg.Bootstrap && len(cfg.Peers) > 0 {
		hasState, err := raft.HasExistingState(store, store, snapshots)
		if err != nil {
			_ = n.Shutdown()
			return nil, fmt.Errorf("raftnode: existing state: %w", err)
		}
		if !hasState {
			servers := make([]raft.Server, 0, len(cfg.Peers))
			for _, p := range cfg.Peers {
				servers = append(servers, raft.Server{
					ID:      raft.ServerID(p.ID),
					Address: raft.ServerAddress(p.RaftAddr),
				})
			}
			if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
				_ = n.Shutdown()
				return nil, fmt.Errorf("raftnode: bootstrap: %w", err)
			}
			logger.Info("cluster bootstrapped", "servers", len(servers))
		}
	}

	return n, nil
}

// IsLeader reports whether this node is the current leader.
func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }

// LeaderWithID returns the leader's raft address and id, empty when unknown.
func (n *Node) LeaderWithID() (string, string) {
	addr, id := n.raft.LeaderWithID()
	return string(addr), string(id)
}

// State returns the raft role as a string.
func (n *Node) State() string { return n.raft.State().String() }

// AppliedIndex is the last index applied to the FSM.
func (n *Node) AppliedIndex() uint64 { return n.raft.AppliedIndex() }

// LastIndex is the last index present in the log.
func (n *Node) LastIndex() uint64 { return n.raft.LastIndex() }

// CommitIndex is the highest log entry known to be committed.
func (n *Node) CommitIndex() uint64 { return n.raft.CommitIndex() }

// Apply proposes data through the leader and waits until the FSM has applied
// it. The value returned by the FSM is passed through unchanged: rejecting a
// command after it is committed is the FSM's decision, and the HTTP layer needs
// the reason to answer with the right status instead of a blanket 200.
func (n *Node) Apply(data []byte, timeout time.Duration) (uint64, any, error) {
	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return 0, nil, err
	}
	return future.Index(), future.Response(), nil
}

// Shutdown stops raft and closes its resources.
func (n *Node) Shutdown() error {
	var firstErr error
	if n.raft != nil {
		if err := n.raft.Shutdown().Error(); err != nil {
			firstErr = err
		}
	}
	if n.transport != nil && n.closer != nil {
		if err := n.closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if n.store != nil {
		if err := n.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
