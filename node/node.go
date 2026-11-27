// Package node assembles a github.com/hashicorp/raft Raft instance with
// sane defaults. It does not wrap raft: the state-machine contract is
// raft.FSM itself, and the returned Node exposes the underlying *raft.Raft.
//
// Two orthogonal choices are made through Config: the storage backend
// (in-memory, or durable bbolt log/stable store plus file snapshots under
// DataDir) and the transport (in-memory, or TCP at BindAddr, optionally
// wrapped in TLS).
package node

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	raftboltdb "github.com/rqlite/raft-boltdb/v2"

	"github.com/hashicorp/raft"
)

// Config describes a node. FSM is the only mandatory field besides ID, and
// exactly one of InMemory or DataDir must select the storage backend.
type Config struct {
	// ID is the unique server ID of this node.
	ID string
	// Address is the transport address. For the in-memory transport it
	// defaults to ID; for TCP it is the optional advertised address and
	// defaults to the bound address.
	Address string
	// FSM is the replicated state machine (raft.FSM, used as is).
	FSM raft.FSM
	// InMemory selects the in-memory log, stable and snapshot stores.
	InMemory bool
	// DataDir selects the durable backend: a bbolt LogStore+StableStore
	// (raft.db) and a raft.FileSnapshotStore (snapshots/) under this
	// directory, which is created if missing. Mutually exclusive with
	// InMemory.
	DataDir string
	// BindAddr, when set, makes the transport a TCP raft.NetworkTransport
	// listening there (host:port; port 0 picks a free one, and the host must
	// be a concrete IP unless Address advertises one). Empty selects the
	// in-memory transport.
	BindAddr string
	// TLS, when non-nil, wraps the TCP transport in TLS. The same config is
	// used to accept (Certificates, ClientCAs, ClientAuth) and to dial
	// (RootCAs, Certificates). It requires BindAddr.
	TLS *tls.Config
	// Bootstrap makes this node a single-voter cluster on first start; it is
	// a no-op when the stores already hold raft state (a restart).
	Bootstrap bool
	// Raft optionally overrides the defaults; LocalID is always set to ID.
	Raft *raft.Config
}

// Node is an assembled raft instance.
type Node struct {
	// Raft is the underlying hashicorp raft, exposed unwrapped.
	Raft *raft.Raft
	// Transport is the transport in use: *raft.InmemTransport (for
	// connecting test peers) or *raft.NetworkTransport.
	Transport raft.Transport

	closers []io.Closer
}

const (
	snapshotsRetained = 2
	tcpMaxPool        = 3
	tcpTimeout        = 10 * time.Second
)

// New validates cfg, assembles the stores and transport, and starts raft.
func New(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("node: empty ID")
	}
	if cfg.FSM == nil {
		return nil, errors.New("node: nil FSM")
	}
	if cfg.InMemory == (cfg.DataDir != "") {
		return nil, errors.New("node: set exactly one of InMemory or DataDir")
	}
	if cfg.TLS != nil && cfg.BindAddr == "" {
		return nil, errors.New("node: TLS requires BindAddr")
	}
	rc := raft.DefaultConfig()
	if cfg.Raft != nil {
		c := *cfg.Raft
		rc = &c
	}
	rc.LocalID = raft.ServerID(cfg.ID)

	n := &Node{}
	fail := func(err error) (*Node, error) {
		n.close()
		return nil, err
	}

	var (
		logs  raft.LogStore
		stab  raft.StableStore
		snaps raft.SnapshotStore
	)
	if cfg.InMemory {
		m := raft.NewInmemStore()
		logs, stab, snaps = m, m, raft.NewInmemSnapshotStore()
	} else {
		if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
			return fail(fmt.Errorf("node: data dir: %w", err))
		}
		b, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
		if err != nil {
			return fail(fmt.Errorf("node: bolt store: %w", err))
		}
		n.closers = append(n.closers, b)
		fs, err := raft.NewFileSnapshotStore(cfg.DataDir, snapshotsRetained, os.Stderr)
		if err != nil {
			return fail(fmt.Errorf("node: snapshot store: %w", err))
		}
		logs, stab, snaps = b, b, fs
	}

	trans, err := newTransport(cfg)
	if err != nil {
		return fail(err)
	}
	n.Transport = trans
	if c, ok := trans.(io.Closer); ok {
		n.closers = append(n.closers, c)
	}

	if cfg.Bootstrap {
		has, err := raft.HasExistingState(logs, stab, snaps)
		if err != nil {
			return fail(fmt.Errorf("node: inspect state: %w", err))
		}
		if !has {
			err := raft.BootstrapCluster(rc, logs, stab, snaps, trans, raft.Configuration{
				Servers: []raft.Server{{Suffrage: raft.Voter, ID: rc.LocalID, Address: trans.LocalAddr()}},
			})
			if err != nil {
				return fail(fmt.Errorf("node: bootstrap: %w", err))
			}
		}
	}
	r, err := raft.NewRaft(rc, cfg.FSM, logs, stab, snaps, trans)
	if err != nil {
		return fail(fmt.Errorf("node: new raft: %w", err))
	}
	n.Raft = r
	return n, nil
}

// newTransport builds the in-memory, TCP or TCP+TLS transport.
func newTransport(cfg Config) (raft.Transport, error) {
	if cfg.BindAddr == "" {
		addr := raft.ServerAddress(cfg.Address)
		if addr == "" {
			addr = raft.ServerAddress(cfg.ID)
		}
		_, t := raft.NewInmemTransport(addr)
		return t, nil
	}
	var adv net.Addr
	if cfg.Address != "" {
		a, err := net.ResolveTCPAddr("tcp", cfg.Address)
		if err != nil {
			return nil, fmt.Errorf("node: advertise address: %w", err)
		}
		adv = a
	}
	nc := &raft.NetworkTransportConfig{MaxPool: tcpMaxPool, Timeout: tcpTimeout}
	if cfg.TLS == nil {
		t, err := raft.NewTCPTransportWithConfig(cfg.BindAddr, adv, nc)
		if err != nil {
			return nil, fmt.Errorf("node: tcp transport: %w", err)
		}
		return t, nil
	}
	l, err := tls.Listen("tcp", cfg.BindAddr, cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("node: tls listen: %w", err)
	}
	s := &tlsStreamLayer{Listener: l, advertise: adv, cfg: cfg.TLS}
	if a, ok := s.Addr().(*net.TCPAddr); !ok || a.IP == nil || a.IP.IsUnspecified() {
		l.Close()
		return nil, errors.New("node: bind address is not advertisable; set Address")
	}
	nc.Stream = s
	return raft.NewNetworkTransportWithConfig(nc), nil
}

func (n *Node) close() error {
	var err error
	for i := len(n.closers) - 1; i >= 0; i-- {
		if cerr := n.closers[i].Close(); err == nil {
			err = cerr
		}
	}
	n.closers = nil
	return err
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

// Leader returns the address and server ID of the current leader, both ""
// if unknown.
func (n *Node) Leader() (addr, id string) {
	a, i := n.Raft.LeaderWithID()
	return string(a), string(i)
}

// Shutdown stops raft, waits for it to finish, then closes the transport and
// any durable stores.
func (n *Node) Shutdown() error {
	err := n.Raft.Shutdown().Error()
	if cerr := n.close(); err == nil {
		err = cerr
	}
	return err
}
