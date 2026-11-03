package api_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/Alexby9511/SD-Calendario/server/internal/api"
	"github.com/Alexby9511/SD-Calendario/server/internal/fsm"
	"github.com/Alexby9511/SD-Calendario/server/internal/raftnode"
)

// These tests run the real raftnode with in-memory transports and real HTTP
// listeners. They cover what a fake cannot: forwarding through an actual TCP
// request and admission leaving the replicated log untouched.

type clusterNode struct {
	id      string
	url     string
	node    *raftnode.Node
	machine *fsm.Echo
}

type testCluster struct {
	nodes []*clusterNode
}

func startCluster(t *testing.T, size int) *testCluster {
	t.Helper()

	ids := make([]string, size)
	listeners := make([]net.Listener, size)
	peers := make([]raftnode.Peer, size)
	transports := make([]*raft.InmemTransport, size)

	// Allocate every listener first: the HTTP address of a node is part of
	// every peer table, so it must be known before the first node starts.
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i+1)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = ln
		peers[i] = raftnode.Peer{ID: ids[i], RaftAddr: ids[i], HTTPAddr: ln.Addr().String()}
	}

	for i, id := range ids {
		_, tr := raft.NewInmemTransport(raft.ServerAddress(id))
		transports[i] = tr
	}
	for i := range ids {
		for j := range ids {
			if i != j {
				transports[i].Connect(raft.ServerAddress(ids[j]), transports[j])
			}
		}
	}

	cluster := &testCluster{}
	for i := range ids {
		machine := fsm.New()
		node, err := raftnode.New(raftnode.Config{
			ID:        ids[i],
			RaftAddr:  ids[i],
			DataDir:   t.TempDir(),
			Bootstrap: i == 0,
			Peers:     peers,
			FSM:       machine,
			Transport: transports[i],
			Logger:    quietLogger(),
		})
		if err != nil {
			t.Fatalf("start %s: %v", ids[i], err)
		}

		httpServer := &http.Server{
			Handler: api.New(api.Config{
				Node:     node,
				NodeID:   ids[i],
				HTTPAddr: peers[i].HTTPAddr,
				Peers:    peers,
				Lookup:   machine.Get,
				Validate: machine.Validate,
				Logger:   quietLogger(),
			}).Handler(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		srv := httpServer
		ln := listeners[i]
		go func() { _ = srv.Serve(ln) }()
		t.Cleanup(func() {
			_ = srv.Close()
			_ = node.Shutdown()
		})

		cluster.nodes = append(cluster.nodes, &clusterNode{
			id:      ids[i],
			url:     "http://" + peers[i].HTTPAddr,
			node:    node,
			machine: machine,
		})
	}
	return cluster
}

func (c *testCluster) waitLeader(t *testing.T, timeout time.Duration) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n.node.IsLeader() {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader after %s", timeout)
	return nil
}

func (c *testCluster) follower(leader *clusterNode) *clusterNode {
	for _, n := range c.nodes {
		if n.id != leader.id {
			return n
		}
	}
	return nil
}

func (c *testCluster) waitValue(t *testing.T, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, n := range c.nodes {
			if got, ok := n.machine.Get(key); !ok || got != want {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, n := range c.nodes {
		got, ok := n.machine.Get(key)
		t.Logf("%s: %q present=%v", n.id, got, ok)
	}
	t.Fatalf("not every node converged on %s=%s", key, want)
}

func (c *testCluster) appliedIndex(t *testing.T, n *clusterNode) uint64 {
	t.Helper()
	resp, err := http.Get(n.url + "/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		AppliedIndex uint64 `json:"applied_index"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body.AppliedIndex
}

func TestClusterCommitsThroughFollower(t *testing.T) {
	cluster := startCluster(t, 2)
	leader := cluster.waitLeader(t, 10*time.Second)
	follower := cluster.follower(leader)
	if follower == nil {
		t.Fatal("no follower")
	}

	resp := postCommand(t, follower.url, `{"op":"set","key":"a","value":"1"}`)
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %s", resp.StatusCode, raw)
	}
	var body struct {
		Status   string `json:"status"`
		LogIndex uint64 `json:"log_index"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "committed" || body.LogIndex == 0 {
		t.Fatalf("body = %+v", body)
	}

	// Committed is not enough: the value must be in every FSM.
	cluster.waitValue(t, "a", "1", 5*time.Second)
}

// H1 end to end: admission rejects the command on the follower that received
// it, so no raft entry is created anywhere.
func TestClusterAdmissionConsumesNoLogEntry(t *testing.T) {
	cluster := startCluster(t, 2)
	leader := cluster.waitLeader(t, 10*time.Second)
	follower := cluster.follower(leader)
	if follower == nil {
		t.Fatal("no follower")
	}

	before := cluster.appliedIndex(t, leader)

	resp := postCommand(t, follower.url, `{"op":"explode","key":"a"}`)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %s", resp.StatusCode, raw)
	}

	// Give any (incorrect) concurrent commit time to land.
	time.Sleep(100 * time.Millisecond)
	if after := cluster.appliedIndex(t, leader); after != before {
		t.Fatalf("applied index moved from %d to %d: the rejected command was logged", before, after)
	}
	if _, ok := leader.machine.Get("a"); ok {
		t.Fatal("rejected command mutated the FSM")
	}
}
