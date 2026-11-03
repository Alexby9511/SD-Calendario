package raftnode_test

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/Alexby9511/SD-Calendario/server/internal/fsm"
	"github.com/Alexby9511/SD-Calendario/server/internal/raftnode"
)

// TestClusterSurvivesLeaderLoss is the milestone-1 acceptance test: three
// nodes, one command committed, the leader killed, and the remaining two must
// still commit. It uses in-memory transports so it runs in `go test` without
// ports or containers.
func TestClusterSurvivesLeaderLoss(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}

	transports := make(map[string]*raft.InmemTransport, len(ids))
	for _, id := range ids {
		_, tr := raft.NewInmemTransport(raft.ServerAddress(id))
		transports[id] = tr
	}
	// Fully connect the mesh before any node starts talking.
	for _, from := range ids {
		for _, to := range ids {
			if from != to {
				transports[from].Connect(raft.ServerAddress(to), transports[to])
			}
		}
	}

	peers := make([]raftnode.Peer, 0, len(ids))
	for _, id := range ids {
		peers = append(peers, raftnode.Peer{ID: id, RaftAddr: id, HTTPAddr: id + ":8080"})
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	nodes := make(map[string]*raftnode.Node, len(ids))
	machines := make(map[string]*fsm.Echo, len(ids))

	for i, id := range ids {
		machine := fsm.New()
		node, err := raftnode.New(raftnode.Config{
			ID:        id,
			RaftAddr:  id,
			DataDir:   filepath.Join(t.TempDir(), id),
			Bootstrap: i == 0,
			Peers:     peers,
			FSM:       machine,
			Transport: transports[id],
			Logger:    logger,
		})
		if err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
		nodes[id] = node
		machines[id] = machine
		t.Cleanup(func() { _ = node.Shutdown() })
	}

	leaderID, leader := waitForLeader(t, nodes, 10*time.Second)
	applySet(t, leader, "a", "1")
	waitForValue(t, machines, "a", "1", 5*time.Second)

	if err := leader.Shutdown(); err != nil {
		t.Fatalf("shutdown leader %s: %v", leaderID, err)
	}
	delete(nodes, leaderID)
	delete(machines, leaderID)
	t.Logf("leader %s shut down", leaderID)

	_, survivor := waitForLeader(t, nodes, 10*time.Second)
	applySet(t, survivor, "b", "2")
	waitForValue(t, machines, "b", "2", 5*time.Second)
}

func waitForLeader(t *testing.T, nodes map[string]*raftnode.Node, timeout time.Duration) (string, *raftnode.Node) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, node := range nodes {
			if node.IsLeader() {
				return id, node
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return "", nil
}

func applySet(t *testing.T, node *raftnode.Node, key, value string) {
	t.Helper()
	raw, err := json.Marshal(fsm.Command{Op: "set", Key: key, Value: value})
	if err != nil {
		t.Fatal(err)
	}
	index, err := node.Apply(raw, 5*time.Second)
	if err != nil {
		t.Fatalf("apply %s=%s: %v", key, value, err)
	}
	if index == 0 {
		t.Fatal("apply returned log index 0")
	}
}

// waitForValue waits until every node has applied the write. This is what
// distinguishes "the leader accepted it" from "the cluster converged".
func waitForValue(t *testing.T, machines map[string]*fsm.Echo, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, machine := range machines {
			got, ok := machine.Get(key)
			if !ok || got != want {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for id, machine := range machines {
		got, ok := machine.Get(key)
		t.Logf("%s: %q (present=%v)", id, got, ok)
	}
	t.Fatalf("not all nodes converged on %s=%s", key, want)
}
