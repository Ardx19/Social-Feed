# Phase 5 — Resilience Works

> Learning target: keeping a distributed system alive when nodes die — async replication, heartbeats, failover, and a write queue.

## What we built

*(filled in after the phase)*

## Concepts learned

*(async vs sync replication, eventual consistency, absolute-final-values replication, heartbeat / failure detection windows, read failover, write queue + idempotent replay, CAP felt concretely)*

## Key decisions + why

| Decision | Why | Rejected alternative |
|----------|-----|----------------------|
| Async, fire-and-forget replication | Keeps write latency off the critical path | Synchronous (ack from replica) |
| Replicate final values, never operations | Op replay would double-apply increments | Replicate ops |
| **Unit of work = the transaction, not the key** | One primary txn → one payload (whole write-set: edge + count together) → one replica txn. Per-key emission lets a failover window or overflow split edge/count, violating the count invariant and evading self-healing (repeat follow = no-op, never re-sends) | Per-key payloads |
| One ordered channel per replica, single drain goroutine | Per-key order by construction — per-write goroutines let count 5→6, 6→7 arrive 7-then-6 | One goroutine per payload |
| Enqueue non-blocking; overflow drops NEWEST | Replication never throttles primary writes; stale keys self-heal on the next write (absolute values) | Blocking enqueue (replication becomes synchronous) |
| `ROLE=replica` never constructs a replicator | Same binary; a replica replicating onward hammers a non-existent chain | — |
| Replicator ≠ coordinator write-queue | Replicator drops on overflow (stale replica); queue must not drop (503). Same shape, opposite guarantees — name them distinctly | Conflating them |
| Heartbeat 5s, 3 misses → down | Bounded detection window (~15s) | Immediate detection (impossible) |
| Reads fail over to replica on down | Availability during the detection window | Fail hard |
| Write queue covers only offline-materializable writes | Post creation's gate lives on the down shard; only A-side edges/unfollows/deletes queue | Queue everything (silent fan-out loss) |
| Queue overflow → 503, never silent drop | Acknowledged loss on a 200 is the worst outcome | Drop silently |
| Replica mapping via `REPLICAS` env | Coordinator must know each shard's failover target | No mapping (unimplementable failover) |

## Structure map

- `internal/replication/replicator.go` — primary→replica payloads
- `internal/api/heartbeat.go` — monitor loop
- `internal/api/writequeue.go` — bounded queue with drain
- `internal/api/coordinator.go` / `client.go` — failover routing

## Gotchas & edge cases

*(the 15s window where the system is still serving from a dead primary, stale replica reads, queue loss on coordinator restart, no backfill for posts missed during outage)*

## Interview talking points
