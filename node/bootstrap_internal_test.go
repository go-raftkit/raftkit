package node

import (
	"errors"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

// brokenLogs is a LogStore whose reads fail, to reach the state-inspection
// error that real stores raise only on I/O failure.
type brokenLogs struct{ *raft.InmemStore }

func (brokenLogs) LastIndex() (uint64, error) { return 0, errors.New("disk gone") }

func TestBootstrapIfFreshInspectError(t *testing.T) {
	m := raft.NewInmemStore()
	_, trans := raft.NewInmemTransport("a")
	defer trans.Close()
	rc := raft.DefaultConfig()
	rc.LocalID = "a"
	err := bootstrapIfFresh(rc, brokenLogs{m}, m, raft.NewInmemSnapshotStore(), trans)
	if err == nil || !strings.Contains(err.Error(), "inspect state") {
		t.Fatalf("err = %v, want inspect state error", err)
	}
}
