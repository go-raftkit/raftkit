// Package node assembles a github.com/hashicorp/raft Raft instance with
// sane defaults. It does not wrap raft: the state-machine contract is
// raft.FSM itself, and the returned Node exposes the underlying *raft.Raft.
package node

import (
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

// Config describes a node. FSM is the only mandatory field besides ID.
type Config struct {
	// ID is the unique server ID of this node.
	ID string
	// Address is the transport address; defaults to ID for the
	// in-memory backend.
	Address string
	// FSM is the replicated state machine (raft.FSM, used as is).
	FSM raft.FSM
	// InMemory selects the in-memory log, stable and snapshot stores and
	// the in-memory transport. It is the only backend in v0 and must be true.
	InMemory bool
	// Bootstrap makes this node a single-voter cluster on first start.
	Bootstrap bool
	// Raft optionally overrides the defaults; LocalID is always set to ID.
	Raft *raft.Config
}

// Node is an assembled raft instance.
type Node struct {
	// Raft is the underlying hashicorp raft, exposed unwrapped.
	Raft *raft.Raft
	// Transport is the in-memory transport (for connecting test peers).
	Transport *raft.InmemTransport
}

// New validates cfg, assembles the stores and transport, and starts raft.
func New(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("node: empty ID")
	}
	if cfg.FSM == nil {
		return nil, errors.New("node: nil FSM")
	}
	if !cfg.InMemory {
		return nil, errors.New("node: only the in-memory backend exists in v0 (set InMemory)")
	}
	rc := raft.DefaultConfig()
	if cfg.Raft != nil {
		c := *cfg.Raft
		rc = &c
	}
	rc.LocalID = raft.ServerID(cfg.ID)

	addr := raft.ServerAddress(cfg.Address)
	if addr == "" {
		addr = raft.ServerAddress(cfg.ID)
	}
	store := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()
	_, trans := raft.NewInmemTransport(addr)

	if cfg.Bootstrap {
		err := raft.BootstrapCluster(rc, store, store, snaps, trans, raft.Configuration{
			Servers: []raft.Server{{Suffrage: raft.Voter, ID: rc.LocalID, Address: trans.LocalAddr()}},
		})
		if err != nil {
			return nil, fmt.Errorf("node: bootstrap: %w", err)
		}
	}
	r, err := raft.NewRaft(rc, cfg.FSM, store, store, snaps, trans)
	if err != nil {
		return nil, fmt.Errorf("node: new raft: %w", err)
	}
	return &Node{Raft: r, Transport: trans}, nil
}

// Apply proposes cmd to the replicated log and waits for it to be applied
// on this node, returning the FSM's Apply result.
func (n *Node) Apply(cmd []byte, timeout time.Duration) (any, error) {
	f := n.Raft.Apply(cmd, timeout)
	if err := f.Error(); err != nil {
		return nil, err
	}
	return f.Response(), nil
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool { return n.Raft.State() == raft.Leader }

// Leader returns the address of the current leader, or "" if unknown.
func (n *Node) Leader() string {
	a, _ := n.Raft.LeaderWithID()
	return string(a)
}

// Shutdown stops raft and waits for it to finish.
func (n *Node) Shutdown() error { return n.Raft.Shutdown().Error() }
