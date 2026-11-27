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

- `node` (v0): `Config` + `New(cfg)` assembling a `raft.Raft` with an
  in-memory backend (log, stable, snapshots, transport); `Apply`, `IsLeader`,
  `Leader`, `Shutdown`; the unwrapped `*raft.Raft` stays reachable.
- Planned: durable log/stable/snapshot stores, a network transport,
  multi-node membership helpers, an admin surface.

## CGO

`CGO_ENABLED=0`: the library and its tests build and run without cgo. (The
Go race detector itself needs cgo, so only the `-race` CI step enables it.)

## Development

```
go vet ./... && gofmt -l . && go test -race ./...
```

Go version pinned to 1.27.4 in `go.mod` and CI.
