# Phase 5 — Resilience Works

> Learning target: keeping a distributed system alive when nodes die — async replication, heartbeats, failover, and a write queue.

## What we built

*(replication done in U9; heartbeat/failover/queue land in U10)*

## Concepts learned

- **Async vs sync replication:** the primary acknowledges a write before the replica confirms — write latency stays off the critical path, at the cost of a staleness window.
- **Eventual consistency:** the replica converges to the primary's state over time, not atomically.
- **Absolute final values, never operations:** replicating "increment the count" would double-apply on replay; replicating the final computed value is idempotent.
- **Unit of work = the transaction, not the key:** one primary txn → one op → one replica txn. The follower edge and its count ride together so the replica never sees them split.
- **Ordered delivery:** a single drain goroutine over one channel preserves per-key order. Per-write goroutines would let count 5→6, 6→7 arrive 7-then-6 and settle the replica wrong.
- **Fire-and-forget with drop-newest:** replication never blocks the primary write; on overflow the newest op is dropped and the replica self-heals on the next write (absolute values re-send the truth).
- **Replicator ≠ write queue:** same shape, opposite guarantees — the replicator drops on overflow (stale replica, availability gap); the coordinator write-queue (U10) must never drop (503).

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

## Structure map

- `internal/replication/replicator.go` — ordered channel, single drain, drop-newest, POSTs absolute values
- `internal/storage` — commit hook emitting each txn's full WriteSet; `ApplyWriteSet` for the replica
- `internal/api/shard.go` — `/replicate` endpoint (applies a write-set as one txn)
- `cmd/app` — primary wires the replicator from `REPLICA` env; replica does not

## Gotchas & edge cases

- **The URL needs a scheme:** the replicator must build `http://` + host:port + `/replicate`; passing a bare `host:port` breaks the POST. Caught by a test.
- **Store writes replicate via the hook, not the handler:** the commit hook fires on `Batch`/`Delete`/`CreateUser`/`AddFollower`/`RemoveFollower` — so a follow-edge's edge + count land as one op. `ApplyWriteSet` (the replica side) does NOT notify, so a replica never replicates onward.
- **Idempotent no-ops don't replicate:** a duplicate follow emits nothing (nothing was written), so it can't heal a prior drop — which is exactly why the edge+count must ride together in the original op.

## Interview talking points

- "How do you avoid data loss with async replication?" — you don't fully; that's the tradeoff. Fire-and-forget means a window of possible loss (the accepted gap, `design.md`). You bound it by replicating absolute values so retries are idempotent and self-healing.
- "Why final values and not operations?" — replaying "increment" double-applies; replaying the final count is idempotent. That's why the transaction's whole write-set is the unit.
- "How do you keep a replica's per-key order?" — one ordered channel, one drain goroutine. Ordering by construction beats trying to order N concurrent POSTs.
- "What happens on buffer overflow?" — the newest op is dropped and logged; the replica is stale on those keys until the next write re-sends a fresh absolute value. Self-healing under traffic.
- "Why can't a replica replicate onward?" — same binary; the role gate at startup prevents a replication loop.
