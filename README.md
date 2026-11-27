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
