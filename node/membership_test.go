package node_test

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-raftkit/raftkit/node"
	"github.com/hashicorp/raft"
)

// snapFSM is an FSM with a genuine Snapshot/Restore: the snapshot serializes
// every command, Restore replaces state with the decoded list. It records
// separately which commands reached Apply (log replay) and whether Restore
// ran (InstallSnapshot), so tests can tell the two catch-up paths apart.
type snapFSM struct {
	mu       sync.Mutex
	cmds     []string // full state
	applied  []string // commands delivered through Apply only
	restored bool
	ch       chan string   // one signal per Apply
	rest     chan struct{} // one signal per Restore
}

func newSnapFSM() *snapFSM {
	return &snapFSM{ch: make(chan string, 64), rest: make(chan struct{}, 4)}
}

func (f *snapFSM) Apply(l *raft.Log) any {
	f.mu.Lock()
	f.cmds = append(f.cmds, string(l.Data))
	f.applied = append(f.applied, string(l.Data))
	f.mu.Unlock()
	f.ch <- string(l.Data)
	return nil
}

func (f *snapFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &cmdSnap{cmds: slices.Clone(f.cmds)}, nil
}

func (f *snapFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var cmds []string
	if err := json.NewDecoder(rc).Decode(&cmds); err != nil {
		return err
	}
	f.mu.Lock()
	f.cmds = cmds
	f.restored = true
	f.mu.Unlock()
	f.rest <- struct{}{}
	return nil
}

func (f *snapFSM) state() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.cmds)
}

func (f *snapFSM) waitApply(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-f.ch:
		if got != want {
			t.Fatalf("applied %q, want %q", got, want)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("%q not applied within 15s", want)
	}
}

type cmdSnap struct{ cmds []string }

func (s *cmdSnap) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.cmds); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}
func (s *cmdSnap) Release() {}

// member starts a durable TCP node under its own DataDir.
func member(t *testing.T, id string, fsm raft.FSM, bootstrap bool, rc *raft.Config) *node.Node {
	t.Helper()
	n, err := node.New(node.Config{
		ID:        id,
		FSM:       fsm,
		DataDir:   filepath.Join(t.TempDir(), "raft"),
		BindAddr:  "127.0.0.1:0",
		Bootstrap: bootstrap,
		Raft:      rc,
	})
	if err != nil {
		t.Fatalf("node %s: %v", id, err)
	}
	t.Cleanup(func() { n.Shutdown() })
	return n
}

func addVoter(t *testing.T, leader, joiner *node.Node, id string) {
	t.Helper()
	// A leader is the only node that may change membership; Leader() gives the
	// address a caller who is not the leader must redirect to.
	if !leader.IsLeader() {
		t.Fatal("membership change needs the leader")
	}
	f := leader.Raft.AddVoter(raft.ServerID(id), joiner.Transport.LocalAddr(), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		t.Fatalf("AddVoter %s: %v", id, err)
	}
}

func voters(t *testing.T, n *node.Node) []string {
	t.Helper()
	f := n.Raft.GetConfiguration()
	if err := f.Error(); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, s := range f.Configuration().Servers {
		if s.Suffrage == raft.Voter {
			ids = append(ids, string(s.ID))
		}
	}
	slices.Sort(ids)
	return ids
}

// liveCluster starts a bootstrapped leader "a" and two voters, "b" and "c",
// with a compaction-friendly config on every node.
func liveCluster(t *testing.T) (nodes []*node.Node, fsms []*snapFSM) {
	t.Helper()
	notify := make(chan bool, 4)
	for i, id := range []string{"a", "b", "c"} {
		rc := raft.DefaultConfig()
		rc.TrailingLogs = 2 // keep almost nothing after a snapshot
		rc.SnapshotThreshold = 1 << 30
		if i == 0 {
			rc.NotifyCh = notify
		}
		f := newSnapFSM()
		fsms = append(fsms, f)
		nodes = append(nodes, member(t, id, f, i == 0, rc))
	}
	select {
	case ok := <-notify:
		if !ok {
			t.Fatal("notified of leadership loss, want gain")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("no leadership within 15s")
	}
	addVoter(t, nodes[0], nodes[1], "b")
	addVoter(t, nodes[0], nodes[2], "c")
	return nodes, fsms
}

func TestLiveJoinConverges(t *testing.T) {
	nodes, fsms := liveCluster(t)
	for i := range 3 {
		if _, err := nodes[0].Apply([]byte("c"+strconv.Itoa(i)), 10*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range fsms {
		for i := range 3 {
			f.waitApply(t, "c"+strconv.Itoa(i))
		}
	}

	jf := newSnapFSM()
	joiner := member(t, "d", jf, false, nil) // not bootstrapped: empty config
	if got := voters(t, joiner); len(got) != 0 {
		t.Fatalf("unbootstrapped joiner config = %v, want empty", got)
	}
	addVoter(t, nodes[0], joiner, "d")

	if got, want := fmt.Sprint(voters(t, nodes[0])), "[a b c d]"; got != want {
		t.Fatalf("leader config = %s, want %s", got, want)
	}
	for i := range 3 {
		jf.waitApply(t, "c"+strconv.Itoa(i)) // no snapshot: pure log replay
	}
	if _, err := nodes[0].Apply([]byte("after"), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, f := range append(fsms, jf) {
		f.waitApply(t, "after")
	}
	if got, want := jf.state(), fsms[0].state(); !slices.Equal(got, want) {
		t.Fatalf("joiner state %v, leader %v", got, want)
	}
	// The joiner learned the committed configuration through replication.
	if got, want := fmt.Sprint(voters(t, joiner)), "[a b c d]"; got != want {
		t.Fatalf("joiner config = %s, want %s", got, want)
	}
}

// TestJoinCatchesUpViaInstallSnapshot compacts the leader's log past the
// early entries before the new node joins, so the only way the joiner can hold
// them is InstallSnapshot -> FSM.Restore.
func TestJoinCatchesUpViaInstallSnapshot(t *testing.T) {
	nodes, fsms := liveCluster(t)
	const early = 10
	for i := range early {
		if _, err := nodes[0].Apply([]byte("early"+strconv.Itoa(i)), 10*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range fsms {
		for i := range early {
			f.waitApply(t, "early"+strconv.Itoa(i))
		}
	}
	// Snapshot() blocks on its future; compaction to TrailingLogs follows it.
	if err := nodes[0].Raft.Snapshot().Error(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// Non-vacuity 1: the log really is compacted. After a snapshot at index S
	// with TrailingLogs=2 the first log index is S-2, well above 1, so a fresh
	// follower cannot be served early0 from the log.
	st := nodes[0].Raft.Stats()
	snapIdx, err := strconv.ParseUint(st["last_snapshot_index"], 10, 64)
	if err != nil || snapIdx <= early {
		t.Fatalf("last_snapshot_index = %q (%v), want > %d", st["last_snapshot_index"], err, early)
	}

	jf := newSnapFSM()
	joiner := member(t, "d", jf, false, nil)
	addVoter(t, nodes[0], joiner, "d")

	select {
	case <-jf.rest:
	case <-time.After(15 * time.Second):
		t.Fatal("joiner never restored a snapshot")
	}
	if _, err := nodes[0].Apply([]byte("late"), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	jf.waitApply(t, "late")

	// Non-vacuity 2: the joiner holds all early state, yet Apply never saw any
	// of it -- it can only have arrived through Restore.
	jf.mu.Lock()
	restored, applied := jf.restored, slices.Clone(jf.applied)
	jf.mu.Unlock()
	if !restored {
		t.Fatal("Restore was not called")
	}
	for _, c := range applied {
		if len(c) >= 5 && c[:5] == "early" {
			t.Fatalf("early command %q reached Apply: caught up by log replay, not snapshot", c)
		}
	}
	got := jf.state()
	for i := range early {
		if !slices.Contains(got, "early"+strconv.Itoa(i)) {
			t.Fatalf("joiner state %v lacks early%d", got, i)
		}
	}
	if want := fsms[0].state(); !slices.Equal(got, want) {
		t.Fatalf("joiner state %v, leader %v", got, want)
	}
}

func TestRemoveServerLeavesConfiguration(t *testing.T) {
	nodes, fsms := liveCluster(t)
	if _, err := nodes[0].Apply([]byte("before"), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, f := range fsms {
		f.waitApply(t, "before")
	}

	if err := nodes[0].Raft.RemoveServer("c", 0, 10*time.Second).Error(); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	if got, want := fmt.Sprint(voters(t, nodes[0])), "[a b]"; got != want {
		t.Fatalf("leader config = %s, want %s", got, want)
	}
	// The remaining quorum keeps committing; the removed node never sees it.
	if _, err := nodes[0].Apply([]byte("after"), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	fsms[0].waitApply(t, "after")
	fsms[1].waitApply(t, "after")
	select {
	case got := <-fsms[2].ch:
		t.Fatalf("removed node applied %q", got)
	case <-time.After(2 * time.Second):
	}
	if nodes[2].IsLeader() {
		t.Fatal("removed node is leader")
	}
}
