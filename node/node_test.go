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
	n, err := node.New(node.Config{ID: "n1", FSM: fsm, InMemory: true, Bootstrap: true})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()

	deadline := time.Now().Add(10 * time.Second)
	for !n.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("no leadership within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n.Leader() != "n1" {
		t.Fatalf("Leader() = %q, want n1", n.Leader())
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
