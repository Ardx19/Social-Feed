# Distributed Social Feed Engine — Design Document

> Written interleaved with the build (R10): each section's "why" is captured while the reasoning is fresh, not backfilled at the end.

**Status:** complete — 6 phases built and verified.

## 1. Problem

When a user posts, the post must reach every follower's feed. This is the **fan-out problem**. The naive answer — write the post into every follower's feed immediately (fan-out on write) — is write-amplified and unbounded for celebrity accounts: a post from an account with a million followers is copied a million times. The other extreme — compute the feed on read by gathering all followees' posts (fan-out on read) — makes every feed read expensive and slow. Production systems (Twitter being the canonical example) run a **hybrid**: fan out eagerly for normal users, serve celebrities lazily on read.

This project builds that system across a sharded backend, and it exists to make the underlying distributed-systems concepts — consistent hashing, sharding, scatter-gather, CAP tradeoffs, async replication — concrete by building them rather than reading about them.

## 2. Architecture

```
coordinator (Go)
  - consistent hash ring (routing by entity ID)
  - hybrid fan-out (celebrity threshold)
  - health monitor, failover, write queue
      │ HTTP/JSON
  ┌────┴────┬────────┬────────┐
shard1    shard2    shard3     (Go, BadgerDB)
  │repl      │repl     │repl
replica1  replica2  replica3   (async replication)
```

- **Coordinator** owns the ring, routes every entity ID to its shard, runs the fan-out logic, detects failures, and fails reads over to replicas. It is the only component exposed to clients.
- **Shards** each own a subset of the keyspace (by consistent hashing) and store everything in an embedded BadgerDB. They serve reads/writes/scans and the count-bearing graph transactions.
- **Replicas** are full copies of a shard's data, kept eventually-consistent by async replication, and used for failover.

## 3. Key Decisions

### 3.1 Storage engine and key design (Phase 1)

**Why BadgerDB:** embedded, pure Go, LSM tree with efficient range scans. No server to run — each shard process owns its DB. Values under the 1 MB `ValueThreshold` are stored inline in the LSM tree, which suits our small values (the value log is barely exercised). Source: BadgerDB quickstart.

**Why the key schema is a design decision:** BadgerDB stores keys lexicographically. Key design determines what range scans are possible, so it is the most load-bearing decision in the storage layer.

The keys:
- `users:{id}` — user record (msgpack + zstd)
- `posts:{id}` — source-of-truth post (author's shard)
- `posts_by_author:{author}:{ts19}:{post_id}` — **author index** (added per R3/R4; the celebrity read path needs a single-shard scan by author, which `posts:{id}` cannot provide because post IDs hash across shards)
- `feed:{user}:{ts19}:{post_id}` — denormalized feed entry (content copied per follower)
- `graph:followers:{user}:{follower}` / `graph:following:{user}:{followee}` — both edge directions

**Why timestamps are zero-padded to 19 chars:** lexicographic order must equal chronological order. `0000000001718123456` sorts correctly; variable-length timestamps do not. Nanoseconds, coordinator-stamped once per post, so there is one source of truth for ordering and no cross-shard clock skew. Among truly simultaneous posts, ordering is arbitrary-but-deterministic: Go serves POSTs concurrently, so same-nanosecond ties sort by post ID (stable, not creation order — there is no corruption, just arbitrary tie order).

**Why the `:{post_id}` suffix:** two posts landing in the same timestamp slot (tight seed loops) would overwrite each other without it — silent post loss. Post IDs are fixed-width hex (`crypto/rand`), the identical lexicographic-sort invariant as the timestamps, applied one level down.

**Why two edge directions:** "who follows user X" and "who does user X follow" are both O(scan) only if both are stored. One direction forces an O(full scan) for the other.

**Why msgpack + zstd, measured:** values are msgpack-encoded and zstd-compressed. Compression is applied only above a 64-byte threshold (a 1-byte flag records raw vs compressed): zstd frames carry ~10+ bytes of overhead, so compressing a ~40-byte Edge inflates it. Below the threshold values are stored raw. This is the "measured, not assumed" compression decision — silently compressing 20-byte values is indefensible.

### 3.2 Consistent hashing (Phase 2)

**Why a consistent hash ring (hand-implemented, no library):** it is a stated learning goal and the distribution behavior is the whole point — we want to understand why it beats naive modulo. With modulo on N shards, adding/removing a shard moves ~all keys; a ring moves only ~1/N of them.

**How it works:** keys hash onto a circle (a sorted array of `uint32` positions); each physical node owns the arc clockwise from its virtual positions to the next. `GetNode` hashes the key, binary-searches the first position at/after it (wrapping), and returns that physical node.

**Why 150 virtual nodes per physical node:** with one position per node, distribution is uneven (a handful of nodes can own most of the arc). Replicating each node 150 times across the ring evens it out.

**Why MD5, not FNV:** FNV-1a's weak avalanche made sequential keys (`key0`…`key9999`) cluster — one node got ~67% of keys. MD5's strong diffusion spreads both key and node hashes evenly (~33/33/33 across 3 shards).

**Static membership:** `RemoveNode` exists for API completeness but failure handling never calls it — re-hashing on failure would strand every key. Failure means failover to a replica, not ring mutation. This is a deliberate asymmetry: consistent hashing minimizes, not eliminates, the cost of membership change.

**Routing by entity ID:** the coordinator's `ShardFor` hashes only the entity ID (user/author/follower), never a full storage key. This is what makes prefix scans work — `feed:{user}:*` must all live on one shard. Hashing a full key would scatter a range scan across the ring and break the read path.

**Deployment / topology:** 7 containers (coordinator + 3 shards + 3 replicas) from one binary, dispatched by subcommand (`./app shard` with `ROLE=replica` for replicas). The coordinator learns the topology from `SHARDS` and `REPLICAS` env; on the Compose network the service name is the host, so the two-segment form `nodeID:port` suffices. Only the coordinator is published to the host — shard ports are internal, so an external client can never read/write a shard directly. Named volumes persist each node's BadgerDB data across restarts.

### 3.3 Social graph (Phase 3)

**Two edges per follow.** A follow relationship is stored as `graph:following:{A}:{B}` (on A's shard) and `graph:followers:{B}:{A}` (on B's shard), so both "who A follows" and "who follows B" are prefix scans. A follow is therefore a **cross-shard write**, which is not atomic.

**Self-follow.** Creating a user writes both self-edges in one transaction, so a user's own posts flow through the existing fan-out and read paths with zero special cases — a non-celebrity gets their own posts via fan-out (self is in their follower set), and a celebrity gets theirs via the read path (self is in their followee set and qualifies as a celebrity). Self-follow does not increment FollowerCount.

**The count is a celebrity gate, so it must be exact.** FollowerCount decides which fan-out path runs, so it must always equal the number of real follower edges. That forces a **conditional count-bearing transaction**: `AddFollower` checks the follower edge, writes it, and increments the count in ONE transaction — a duplicate follow is an idempotent no-op, and unfollow is conditional the other way. A generic increment endpoint can't back this (edge + count as two transactions can drift apart).

**Concurrency via optimistic concurrency + retry.** BadgerDB uses optimistic concurrency: concurrent writes to the same user record (a celebrity is a hot key) commit-fail with `ErrConflict`, and `db.Update` does not retry. The storage method retries with capped exponential backoff. Each successful commit clears contention, so the loop converges — verified by a 100-goroutine concurrent-follow test. Accepted: every follow of a celebrity serializes through the retry loop (one hot user record); a separate count key would shrink the conflict domain, but the hot key is unavoidable either way.

**The count transaction lives in the storage layer**, not the API handler, so the codec (zstd/msgpack) never leaks into handlers. The HTTP endpoint is a thin call.

**Cross-shard writes accept half-edges.** If the B-side fails after the A-side succeeds, a half-edge remains (A follows B but B doesn't know). This is accepted and logged (R8); the conditional transaction keeps the count consistent with the B-side edge, so the failure mode is a benign miss, never a drifted count.

### 3.4 Fan-out (Phase 4)

**Hybrid fan-out with a static threshold.** `POST /posts` writes the post and its author-index entry to the author's shard in one transaction, then reads FollowerCount. Below the threshold (1000), it fans a denormalized `FeedEntry` out to every follower's shard, one batched write per shard. At/above the threshold it does nothing — the celebrity is served lazily on read. This bounds write amplification: normal posts are cheap to read (already in the feed), celebrity posts don't blast every follower's feed.

**Denormalized content.** `FeedEntry` copies the post content into every follower's feed, so a feed renders in one range scan with no N+1 post fetches. The cost is storage amplification — the main tradeoff. (The alternative, pointer-only entries, forces N post fetches per feed read.)

**Author index.** The celebrity read path needs "recent posts by author" as a single-shard scan, but `posts:{id}` hashes across shards by post ID. `posts_by_author:{author}:{ts}:{post}` makes it a range scan on the author's shard. This is the key that made the celebrity path bounded.

**Coordinator-stamped timestamps.** One nanosecond timestamp per post, passed into every derived write. Single source of truth — no cross-shard clock skew, and the merge compares comparable values. Ties are arbitrary-but-deterministic (secondary post-ID sort).

**Read path merges two sources.** `GET /feed/{user}` reverse-scans the user's fan-out feed, finds which followees are celebrities (live FollowerCount reads, O(following) per feed load — deliberately not cached, because status changes after you follow and a stale flag silently takes the wrong path), scans each celebrity's author index, and merges both sources newest-first, deduping by post ID (kills the threshold-crossing duplicate artifact).

**The cursor is logical, not a physical key.** A merged page spans `feed:{user}:*` and each celebrity's `posts_by_author:*` on possibly different shards, so no single physical key can seek into both. The cursor is the logical `(timestamp, post_id)`; each source reconstructs its own prefix's seek key from it. A physical-key cursor silently works with no celebrities and then drops or repeats celebrity posts across page boundaries — precisely the demo path.

**Post creation is never queued.** Its celebrity gate and fan-out pairs live on the author's shard, so they can't be materialized offline. A queued post would drain as post + index and silently lose fan-out beyond the accepted scope. It returns 502 instead (see 3.5).

### 3.5 Resilience (Phase 5)

**Async replication, absolute final values.** A primary's committed transactions replicate to its replica as **absolute final values** (never operations — replaying "increment" double-applies; replaying the final count is idempotent), fire-and-forget after local commit. The write path never waits for the replica.

**Unit of work is the transaction, not the key.** One primary transaction → one payload → one replica transaction. The follower edge and its incremented count ride in the same payload, so a failover window or an overflow can never split them — the replica never shows an edge without its count. (A per-key hook would allow exactly that split, and a repeat follow — an idempotent no-op — would never heal it.)

**Ordered delivery.** Each replica gets one ordered channel drained by a single goroutine that POSTs in enqueue order. Per-key order by construction — per-write goroutines would let two updates to the same key (count 5→6, then 6→7) arrive 7-then-6 and settle the replica wrong. Enqueue is non-blocking; on overflow the **newest** op is dropped and the replica self-heals on the next write (absolute values re-send the truth). A replica never constructs a replicator (role-gated), preventing a replication loop.

**Heartbeat + failover.** The coordinator pings every node; 3 consecutive misses (~15s) mark it down. Reads to a down primary route to its replica; during the undetected-death window (primary down but not yet marked), reads get one retry via the replica. Ring membership stays static.

**Write queue covers only offline-materializable writes.** A bounded in-memory queue buffers A-side follow edges, unfollows, and deletes to a down primary, replaying them idempotently on recovery. Overflow returns 503 (never a silent drop — acknowledging loss on a 200 is the worst outcome). Post creation is excluded (see 3.4). The queue lives in the coordinator, so a coordinator restart loses it — accepted and documented.

## 4. Tradeoffs

**CAP position.** The system is **AP** — it prioritizes availability over strict consistency:
- **Eventually consistent** by design: async replication means a replica can be stale; the write queue means a just-recovered shard can be behind. Reads may return slightly stale data (a follower edge present but the count not yet updated on the replica).
- **No consensus.** Without Raft, we have no leader election, no split-brain protection, and no strong ordering guarantee across nodes. That is the single biggest tradeoff — it keeps the system simple and "good enough" for the demo, at the cost of correctness guarantees a production system needs.
- **Consistency within a node is strong.** A single BadgerDB transaction is ACID; the count invariant holds within a shard. The consistency gaps are all cross-node (replication lag, half-edges).

**Consistency vs availability choices:**
- **FollowerCount is exact on the primary but stale on a replica.** It's a gate, not a user-facing number, so staleness is tolerable; a wrong count only affects which fan-out path runs, and both paths are correct (just differently efficient).
- **Half-edges are accepted** in exchange for not blocking follow/unfollow on cross-shard coordination.
- **Queue overflow → 503** chooses explicit rejection (availability hit) over silent loss (correctness violation).

## 5. What breaks at scale

- **The celebrity is a hot key.** Every follow of a celebrity serializes through one user record's conflict-retry loop. With a real A-list account this becomes a bottleneck. Production answer: separate the counter into its own key (still a hot key, but only the counter) and/or shard follower lists, and use a dynamic threshold.
- **Write amplification for normal users.** A post from a user with 100k followers is copied 100k times. This is why the celebrity threshold exists; a static threshold 1000 is arbitrary. Production uses a dynamic threshold tuned to observed load.
- **Fan-out latency under a hot poster.** Even batched per shard, fanning out to 100k followers is many shard writes per post; the poster's latency grows with follower count. Production decouples this with an event queue (see §6).
- **The coordinator is a single point of failure.** One coordinator owns routing, fan-out, and the write queue. It dies → the whole system is down. Production runs multiple coordinators behind a load balancer.
- **Range scans grow unbounded.** No pruning of feed or author-index entries; latest-N reads use reverse iteration, but storage grows forever. Production needs TTL/cap-and-prune.
- **Unfollow leaves residue.** Non-celebrity posts already fanned out to a follower's feed persist after an unfollow (no cleanup job), while celebrity posts vanish instantly (read-time). Asymmetric by design; production adds a cleanup job or tombstone-on-read.
- **Replication has a staleness + loss window.** Async fire-and-forget can lose recent writes and serves stale reads during an outage. Bounded by design, not eliminated.
- **Optimistic-concurrency retry storms.** Under heavy concurrent writes to a hot record, retry loops burn CPU. Backoff caps the worst case but doesn't remove the contention.

## 6. What I'd do differently

- **Raft (or a consensus protocol) for leader election and replication ordering.** This is the biggest gap. It would give strong consistency, split-brain protection, and safe failover, at real complexity cost. Acknowledged explicitly — this project deliberately does not implement it.
- **Kafka (or an event log) for fan-out.** Instead of the coordinator fanning out directly, publish a "post created" event and let consumers write to follower feeds asynchronously. This decouples the poster's latency from follower count, makes fan-out retryable, and removes the "partial fan-out is permanent" gap (R7). It is the production answer to both the write-amplification and fan-out-latency problems.
- **Redis (or a feed cache) as a read-through cache.** Feed reads are the hot path; a cache in front of the merge would absorb repeated reads and offload the shards. Skipped for time.
- **gRPC instead of HTTP/JSON for shard-to-shard and coordinator-to-shard calls.** Lower overhead, typed contracts, streaming. HTTP+JSON was chosen for simplicity and debuggability.
- **A dynamic celebrity threshold**, tuned from observed fan-out load rather than a hardcoded 1000.
- **Multiple coordinators** behind a load balancer to remove the coordinator SPOF.
- **Pruning / TTL** on feed and author-index entries.

