// Command sdcald runs one node of the SD Calendario cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Alexby9511/SD-Calendario/server/internal/api"
	"github.com/Alexby9511/SD-Calendario/server/internal/fsm"
	"github.com/Alexby9511/SD-Calendario/server/internal/raftnode"
)

type peersFlag []raftnode.Peer

func (p *peersFlag) String() string {
	parts := make([]string, 0, len(*p))
	for _, peer := range *p {
		parts = append(parts, peer.ID+"@"+peer.RaftAddr+"@"+peer.HTTPAddr)
	}
	return strings.Join(parts, ",")
}

func (p *peersFlag) Set(value string) error {
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		fields := strings.Split(item, "@")
		if len(fields) != 3 {
			return fmt.Errorf("peer %q: want id@raftAddr@httpAddr", item)
		}
		*p = append(*p, raftnode.Peer{ID: fields[0], RaftAddr: fields[1], HTTPAddr: fields[2]})
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "sdcald: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		id            = flag.String("id", "", "node id: n1, n2 or n3")
		raftBind      = flag.String("raft-bind", "0.0.0.0:7000", "raft listen address")
		raftAdvertise = flag.String("raft-advertise", "", "raft advertised address, e.g. n1:7000")
		httpBind      = flag.String("http-bind", "0.0.0.0:8080", "http listen address")
		httpAdvertise = flag.String("http-advertise", "", "http advertised address, e.g. n1:8080")
		dataDir       = flag.String("data", "./data", "data directory")
		bootstrap     = flag.Bool("bootstrap", false, "bootstrap the cluster if the data dir is empty")
	)
	var peers peersFlag
	flag.Var(&peers, "peers", "comma separated id@raftAddr@httpAddr entries")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	switch {
	case *id == "":
		return errors.New("-id is required")
	case *raftAdvertise == "":
		return errors.New("-raft-advertise is required")
	case *httpAdvertise == "":
		return errors.New("-http-advertise is required")
	case len(peers) == 0:
		return errors.New("-peers is required")
	}

	machine := fsm.New()

	node, err := raftnode.New(raftnode.Config{
		ID:        *id,
		BindAddr:  *raftBind,
		RaftAddr:  *raftAdvertise,
		DataDir:   *dataDir,
		Bootstrap: *bootstrap,
		Peers:     peers,
		FSM:       machine,
		Logger:    logger,
	})
	if err != nil {
		return err
	}

	server := api.New(api.Config{
		Node:     node,
		NodeID:   *id,
		HTTPAddr: *httpAdvertise,
		Peers:    peers,
		Lookup:   machine.Get,
		Logger:   logger,
	})

	httpServer := &http.Server{
		Addr:              *httpBind,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("http listening", "addr", *httpBind, "node", *id)
		serveErr <- httpServer.ListenAndServe()
	}()

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("signal received, shutting down", "node", *id)
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	shutdownErr := httpServer.Shutdown(shutdownCtx)
	cancel()
	nodeErr := node.Shutdown()

	switch {
	case runErr != nil:
		return runErr
	case shutdownErr != nil:
		return shutdownErr
	default:
		return nodeErr
	}
}
