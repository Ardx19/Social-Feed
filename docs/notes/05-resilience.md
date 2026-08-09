# Phase 5 — Resilience Works

> Learning target: keeping a distributed system alive when nodes die — async replication, heartbeats, failover, and a write queue.

## What we built

Async replication (U9): a primary's committed transactions replicate to its replica as absolute final values over an ordered channel. Heartbeat + failover + write queue (U10): the coordinator pings every node, marks one down after 3 misses, routes reads to the replica, queues offline-materializable writes to a down primary (replaying on recovery), and never queues post creation (502). Verified live: stopped a shard, reads still served from the replica, posting to it returned 502, and it recovered on restart.

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
| Read failover to replica | Availability during the detection window; one-retry on the undetected-death window | Fail hard |
| Queue covers only offline-materializable writes | A-side follow edges, unfollows, deletes; post creation's gate lives on the down shard so it 502s instead | Queue everything (silent fan-out loss) |
| Queue overflow → 503, never silent drop | Acknowledged loss on a 200 is the worst outcome | Drop silently |

## Structure map

- `internal/replication/replicator.go` — ordered channel, single drain, drop-newest, POSTs absolute values
- `internal/api/heartbeat.go` — HealthMonitor: 3 misses → down, recovery drains the queue
- `internal/api/writequeue.go` — bounded queue (503 on overflow), idempotent replay
- `internal/api/failover.go` — readAddrFor (replica when primary down), readWithFailover (one-retry), writeToShard (queue-eligible), drainQueue
- `internal/api/shard.go` — `/replicate` endpoint (applies a write-set as one txn)
- `internal/storage` — commit hook emitting each txn's full WriteSet; `ApplyWriteSet`
- `cmd/app` — primary wires the replicator from `REPLICA` env; replica does not

## Gotchas & edge cases

- **The URL needs a scheme:** the replicator must build `http://` + host:port + `/replicate`; passing a bare `host:port` breaks the POST. Caught by a test.
- **Store writes replicate via the hook, not the handler:** the commit hook fires on `Batch`/`Delete`/`CreateUser`/`AddFollower`/`RemoveFollower` — so a follow-edge's edge + count land as one op. `ApplyWriteSet` (the replica side) does NOT notify, so a replica never replicates onward.
- **Idempotent no-ops don't replicate:** a duplicate follow emits nothing, so it can't heal a prior drop — which is exactly why the edge+count must ride together in the original op.
- **Post creation is never queued:** its celebrity gate and fan-out pairs need the down shard, so queuing would drain as post+index and silently lose fan-out. It returns 502 instead.
- **Queue loss on coordinator restart** is an accepted gap (documented) — the queue is in-memory.
- **Live-drill lesson:** the heartbeat window is ~15s; a resilience demo must script the wait, and reads fail over to the replica during the outage.

## Interview talking points

- "How do you avoid data loss with async replication?" — you don't fully; that's the tradeoff. Fire-and-forget means a window of possible loss (the accepted gap, `design.md`). You bound it by replicating absolute values so retries are idempotent and self-healing.
- "Why final values and not operations?" — replaying "increment" double-applies; replaying the final count is idempotent. That's why the transaction's whole write-set is the unit.
- "How do you keep a replica's per-key order?" — one ordered channel, one drain goroutine. Ordering by construction beats trying to order N concurrent POSTs.
- "What happens when a shard dies?" — the heartbeat marks it down after 3 misses; reads fail over to the replica; offline-materializable writes queue and replay on recovery; post creation fails with 502 rather than silently losing fan-out. Ring membership stays static.
- "Why 502 for posts and not queue them?" — a queued post would drain without fan-out (the gate + pairs live on the down shard), silently losing delivery beyond the accepted scope.
- "What's the 15s window?" — between the primary dying and the heartbeat marking it down, reads get one retry via the replica, then error; that's the availability tradeoff of bounded detection.
