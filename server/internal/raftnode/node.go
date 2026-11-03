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
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

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

	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return nil, fmt.Errorf("raftnode: data dir: %w", err)
	}

	store, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("raftnode: bolt store: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
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
		tcpTransport, err := raft.NewTCPTransport(cfg.BindAddr, advertise, 5, 10*time.Second, os.Stderr)
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("raftnode: transport: %w", err)
		}
		transport = tcpTransport
		closer = tcpTransport
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.ID)
	// raft's default log level is DEBUG, which floods stderr in tests and in
	// the demo. Warnings are what an operator needs.
	rc.LogLevel = "WARN"
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

// Apply commits data through the leader and waits for the FSM to apply it.
func (n *Node) Apply(data []byte, timeout time.Duration) (uint64, error) {
	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return 0, err
	}
	return future.Index(), nil
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
