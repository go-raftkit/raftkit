# raftkit

A reusable, pure-Go, engine-agnostic consensus kit built **on**
[`github.com/hashicorp/raft`](https://github.com/hashicorp/raft) v1.8.0
(MPL-2.0, pure Go). Licensed BSD-3-Clause.

> Status: v0, scaffold. AI-authored (Claude), not yet human-reviewed.

## Scope

v0 is the consensus kit only. It is engine-agnostic: the SQL / storage engine
of the larger replicated-SQL project lives in a different repository, decided
later. Nothing here knows about SQL.

## Build on, do not wrap

hashicorp/raft already defines `raft.FSM`, `raft.LogStore`, `raft.StableStore`,
`raft.Transport` and `raft.SnapshotStore`. raftkit does **not** re-declare
facade interfaces over them: the agnostic state-machine contract *is*
`raft.FSM`. What raftkit adds is concrete implementations and ergonomic
assembly with sane defaults.

## Packages

- `node`: `Config` + `New(cfg)` assembling a `raft.Raft`; `Apply`, `IsLeader`,
  `Leader`, `Shutdown`; the unwrapped `*raft.Raft` (`Node.Raft`) and the
  transport (`Node.Transport`, a `raft.Transport`) stay reachable. Two
  independent choices:
  - storage: `InMemory`, or `DataDir` for a durable bbolt log + stable store
    (`rqlite/raft-boltdb/v2`, `raft.db`) and `raft.NewFileSnapshotStore`;
    `Bootstrap` is a no-op on an existing state, so a restart replays the log;
  - transport: in-memory by default, TCP (`raft.NewTCPTransportWithConfig`)
    when `BindAddr` is set, wrapped in TLS (a `raft.StreamLayer` over
    `crypto/tls`) when `TLS` is also set. Membership uses
    `Node.Raft.AddVoter` directly; the tests replicate across three nodes.
- Planned: multi-node membership helpers, an admin surface.

## Membership

Membership is plain hashicorp/raft through `Node.Raft`; raftkit adds no
forwarding methods. Only the leader may change the configuration, so a caller
that is not the leader redirects to `n.Leader()` (address, ID).

```go
// Join: start the new node WITHOUT Bootstrap (empty config, own DataDir),
// then on the leader:
err := leader.Raft.AddVoter(raft.ServerID("d"), joiner.Transport.LocalAddr(), 0, 10*time.Second).Error()

// Leave:
err = leader.Raft.RemoveServer("c", 0, 10*time.Second).Error()

// Inspect the committed configuration:
f := leader.Raft.GetConfiguration(); err = f.Error(); servers := f.Configuration().Servers
```

A joiner catches up from the leader: by log replay while the log still holds
its history, otherwise by InstallSnapshot, which calls your `FSM.Restore`. So
`FSM.Snapshot`/`Restore` must round-trip the whole state. The tests
`TestLiveJoinConverges`, `TestJoinCatchesUpViaInstallSnapshot` (log compacted
with `Raft.Snapshot()` and a small `TrailingLogs`; the joiner holds state its
`Apply` never saw) and `TestRemoveServerLeavesConfiguration` prove this on a
live durable TCP cluster.

## CGO

`CGO_ENABLED=0`: the library and its tests build and run without cgo. (The
Go race detector itself needs cgo, so only the `-race` CI step enables it.)

## Development

```
go vet ./...
test -z "$(gofmt -l .)"
CGO_ENABLED=1 go test -race ./...
```

Go version pinned to 1.27.4 in `go.mod` and CI.
