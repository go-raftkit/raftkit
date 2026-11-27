package node_test

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/go-raftkit/raftkit/node"
	"github.com/hashicorp/raft"
)

// logFSM is a tiny example raft.FSM that records applied commands.
type logFSM struct {
	mu   sync.Mutex
	cmds []string
}

func (f *logFSM) Apply(l *raft.Log) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cmds = append(f.cmds, string(l.Data))
	return len(f.cmds)
}
func (f *logFSM) Snapshot() (raft.FSMSnapshot, error) { return nopSnap{}, nil }
func (f *logFSM) Restore(rc io.ReadCloser) error      { return rc.Close() }

type nopSnap struct{}

func (nopSnap) Persist(s raft.SnapshotSink) error { return s.Close() }
func (nopSnap) Release()                          {}

func TestSingleNodeApply(t *testing.T) {
	fsm := &logFSM{}
	notify := make(chan bool, 4)
	rc := raft.DefaultConfig()
	rc.NotifyCh = notify
	n, err := node.New(node.Config{ID: "n1", FSM: fsm, InMemory: true, Bootstrap: true, Raft: rc})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()

	select {
	case leader := <-notify:
		if !leader {
			t.Fatal("notified of leadership loss, want gain")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no leadership within 10s")
	}
	if !n.IsLeader() {
		t.Fatal("IsLeader() = false after notification")
	}
	if addr, id := n.Leader(); addr != "n1" || id != "n1" {
		t.Fatalf("Leader() = (%q, %q), want (n1, n1)", addr, id)
	}
	resp, err := n.Apply([]byte("hello"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if resp != 1 {
		t.Fatalf("response = %v, want 1", resp)
	}
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	if len(fsm.cmds) != 1 || fsm.cmds[0] != "hello" {
		t.Fatalf("applied = %v", fsm.cmds)
	}
}

func TestNewValidation(t *testing.T) {
	for name, c := range map[string]node.Config{
		"no id":     {FSM: &logFSM{}, InMemory: true},
		"no fsm":    {ID: "a", InMemory: true},
		"no memory": {ID: "a", FSM: &logFSM{}},
	} {
		if _, err := node.New(c); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}
