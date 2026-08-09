# Distributed Social Feed Engine — Design Document

> The interview deliverable. Written interleaved with the build (R10): each section's "why" is captured while the reasoning is fresh, not backfilled at the end. This document is what you pull out in system design interviews — it shows deliberate decisions, not accidental ones.

**Status:** in progress (Phase 1 of 6). Sections fill in as phases complete.

## 1. Problem

> Filled in during the design-doc assembly phase (U13).

## 2. Architecture

> Filled in during the design-doc assembly phase (U13).

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

**Static membership:** `RemoveNode` exists for API completeness but failure handling never calls it — re-hashing on failure would strand every key. Failure means failover to a replica, not ring mutation. This is a deliberate, interview-ready asymmetry: consistent hashing minimizes, not eliminates, the cost of membership change.

**Routing by entity ID:** the coordinator's `ShardFor` hashes only the entity ID (user/author/follower), never a full storage key. This is what makes prefix scans work — `feed:{user}:*` must all live on one shard. Hashing a full key would scatter a range scan across the ring and break the read path.

**Deployment / topology:** 7 containers (coordinator + 3 shards + 3 replicas) from one binary, dispatched by subcommand (`./app shard` with `ROLE=replica` for replicas). The coordinator learns the topology from `SHARDS` and `REPLICAS` env; on the Compose network the service name is the host, so the two-segment form `nodeID:port` suffices. Only the coordinator is published to the host — shard ports are internal, so an external client can never read/write a shard directly. Named volumes persist each node's BadgerDB data across restarts.

### 3.3 Social graph (Phase 3)

> Filled in during Phase 3.

### 3.4 Fan-out (Phase 4)

> Filled in during Phase 4.

### 3.5 Resilience (Phase 5)

> Filled in during Phase 5.

## 4. Tradeoffs

> CAP position, consistency vs availability choices. Filled in during Phase 5.

## 5. What breaks at scale

> Honest assessment. Filled in during Phase 6.

## 6. What I'd do differently

> Raft, Kafka, Redis, gRPC. Filled in during Phase 6.
