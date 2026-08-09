---
title: Build Distributed Social Feed Engine
type: feat
status: active
date: 2026-08-10
origin: docs/brainstorms/distributed-social-feed-engine-requirements.md
deepened: 2026-08-10
---

# Build Distributed Social Feed Engine

## Summary

Build the distributed feed engine from `plan.txt` and the requirements doc: a Go coordinator with a hand-implemented consistent hash ring routing to 3 shard nodes (each with a replica), BadgerDB storage with msgpack + zstd encoding, hybrid fan-out with a static celebrity threshold, Docker Compose orchestration, and a React demo frontend — executed across six phases with design contracts the flow analysis surfaced: coordinator-stamped nanosecond timestamps with `:{post_id}` key suffixes, denormalized feed content, conditional count-bearing graph transactions, ordered replication, and an explicit coordinator↔shard error taxonomy. The design doc (the interview deliverable) is written interleaved with the phases.

---

## Problem Frame

A user's post must reach every follower's feed. Eager fan-out is write-amplified and unbounded for celebrities; lazy fan-out makes reads expensive; hybrid approaches are what production systems run. The builder's goal is system design interview prep (see origin: `docs/brainstorms/distributed-social-feed-engine-requirements.md`): the design doc and the ability to defend tradeoffs are the real deliverable, and working code is the evidence the reasoning was lived. The blueprint (`plan.txt`) is thorough but contains one hard contradiction with the requirements doc — its key schema has no author index, making the celebrity read path unimplementable — plus a set of unspecified contracts (timestamps, error semantics, replica mapping, feed content shape) that would surface as Phase-4/Phase-5 walls.

---

## Requirements

- R1. Build per `plan.txt` architecture: Go coordinator, 3 shards + 3 replicas, BadgerDB + zstd, HTTP+JSON, Docker Compose, React+Vite frontend (see origin: R1)
- R2. Six-phase build order, each phase independently verifiable (see origin: R2)
- R3. Author index key `posts_by_author:{author_id}:{timestamp}:{post_id}` maintained on every post write (see origin: R3)
- R4. Celebrity read path uses the author index — never scans all posts (see origin: R4)
- R5. User creation auto-creates self-follow edges in both directions (see origin: R5)
- R6. Users see their own posts in their feed via both fan-out modes (see origin: R6)
- R7. Fan-out failures to down shards are logged and skipped; no backfill (see origin: R7)
- R8. Cross-shard follow writes are not atomic; half-edges accepted and documented (see origin: R8)
- R9. All `plan.txt` known gaps stay gaps and get documented, not built (see origin: R9)
- R10. Design doc written interleaved with the build phases (see origin: R10)
- R11. Frontend has exactly three views; no auth, no state library (see origin: R11)
- R12. Seed data includes at least one celebrity so both fan-out modes are demoable (see origin: R12)

**Origin actors:** A1 (builder — learns concepts, reviews code), A2 (interviewer — reads design doc)
**Origin flows:** F1 (post creation / write path), F2 (feed read / read path), F3 (follow / unfollow)
**Origin acceptance examples:** AE1 (self post in own feed, non-celebrity), AE2 (self post in own feed, celebrity, no fan-out writes), AE3 (celebrity post reaches follower via author index), AE4 (partial fan-out loss on down shard, no backfill)

---

## Scope Boundaries

- All `plan.txt` known gaps remain out of scope: Raft/consensus, synchronous replication, event queue/Kafka, Redis feed cache, gRPC, dynamic threshold, auth, multi-coordinator, split-brain handling (see origin: R9)
- Guaranteed fan-out delivery and atomic follow writes: documented only (see origin: R7, R8)
- Feed backfill / repair on shard recovery: not built (see origin: R7)
- **Unfollow does not clean up already-fanned-out feed entries** (named gap): when Alice unfollows non-celebrity Bob, Bob's old posts stay in `feed:alice:*` indefinitely (no cleanup job); when Alice unfollows a celebrity, those posts vanish immediately because that path is read-time. The asymmetry is deliberate and documented in `design.md` with the production answer (cleanup job / tombstone-on-read)
- Feed and author-index entry pruning (cap-and-delete of old entries): not built; latest-N reads use reverse iteration instead. Growth behavior documented in `design.md` as a production gap
- Post deletion, editing, likes, comments, search: not built (nothing in the blueprint requires them)
- Frontend beyond the three views: not built (see origin: R11)
- Ring membership changes at runtime: never — failure handling is failover-to-replica, not re-hashing

### Deferred to Follow-Up Work

- `ce-compound` learning capture after major milestones (suggested by research; not part of the build)
- A git-identity setup and initial commit convention (`master` branch, no remote yet)

---

## Context & Research

### Relevant Code and Patterns

- No repo code exists — greenfield. The executable blueprint is `plan.txt` (structure, API surface, env contract, build order) and the requirements doc (governing refinements)
- Environment verified: Go 1.25.1, Node 24.16, Docker 29.1.3. Note: npm prints a benign `config prefix cannot be changed from project config` warning from the user's `.npmrc`

### Institutional Learnings

- None — `docs/solutions/` does not exist; no prior decision records

### External References

- BadgerDB v4 API (verified current): `badger.Open(badger.DefaultOptions(path))`; `db.View`/`db.Update`; `txn.NewIterator` with `Seek` + `ValidForPrefix` for prefix scans; `item.Value(func(v []byte) error)`; values below the 1 MB `ValueThreshold` are stored inline in the LSM tree (all our values are tiny, so the value log is barely exercised — worth a design-doc note). Source: https://github.com/dgraph-io/badger/blob/main/docs/quickstart.md
- BadgerDB cursor pagination pattern (seek to a full key, then reset prefix and iterate): same source as above

---

## Key Technical Decisions

- **Coordinator-stamped single nanosecond timestamp per post:** one `time.Now().UnixNano()` per post, zero-padded to 19 chars in keys, passed into every derived write (post, author index, all fan-out entries). Single source of truth — no cross-shard clock skew, and the merge compares comparable values. Precise phrasing for interviews: ordering among truly simultaneous posts is arbitrary-but-deterministic (Go serves POSTs concurrently; same-nanosecond ties sort by post ID — stable, not creation order — there is no corruption, just arbitrary tie order). Resolves the origin's deferred timestamp question
- **Fixed-width post IDs:** post IDs are `crypto/rand` hex of a fixed byte count — the identical insight to zero-padded timestamps: within a timestamp slot, keys sort by post ID, so variable-length IDs would interleave wrong. Stated as a sibling invariant to the timestamp rule
- **`:{post_id}` suffix on feed and author-index keys** (`feed:{user}:{ts19}:{post_id}`, `posts_by_author:{author}:{ts19}:{post_id}`): two posts landing in the same timestamp slot (tight seed loops) overwrite each other without the suffix — silent post loss. The suffix costs nothing and removes the failure class; scans return (ts, post_id) order
- **Denormalized feed content:** `FeedEntry{PostID, AuthorID, Timestamp, Content}` — one range scan renders the feed; celebrity-path `Post`s and fan-out-path entries merge into one shape. Storage amplification is the interview tradeoff (plan.txt's pointer-only entries would force N+1 post fetches per feed read)
- **Routing by entity ID, never the full key:** coordinator hashes `author_id`/`user_id`/`follower_id` only. Author-index keys must route by `hash(author_id)` so they live on the author's shard with the post — otherwise the R4 single-shard scan guarantee silently breaks
- **Ring membership is static:** `RemoveNode` exists in the ring API (per plan.txt) but failure handling never calls it — re-hashing would strand every key. Failure → failover to replica
- **Self-follow semantics:** both self-edges + the user record are written in one shard-local BadgerDB transaction at user creation; self-follow does **not** increment FollowerCount (users are born at 0; celebrities cross at exactly 1000 real followers). Unfollow-self is rejected with 400 — allowing it would permanently darken a celebrity's own feed
- **Conditional count-bearing graph transaction** (lives in the storage package, not the API handler — codec isolation): `follow(A,B)` performs the B-side writes through `storage.AddFollower`, which checks the follower edge, writes it, and increments FollowerCount in ONE shard-local transaction, retried on `badger.ErrConflict` (BadgerDB uses optimistic concurrency; `db.Update` does not retry, so the storage method must). The HTTP handler is a thin call into it. Invariant: FollowerCount always equals the number of real follower edges (excluding the self-edge) — the count is the celebrity gate, so it must mean exactly what it says. Duplicate follows are idempotent no-ops; unfollow is conditional the other way. A generic increment endpoint cannot back this (edge + count as two transactions can drift between them). A-side edge writes need no ceremony — no count rides on them. Accepted contention: every follow of a celebrity conflicts with every other (one hot user record), serialized by retry — fine at this scale; a separate count key is the future decoupling (interview answer, see design.md)
- **Batched fan-out per destination shard:** followers grouped by shard; one HTTP call per shard carrying final key→value pairs, short per-call deadline. Avoids 1000 concurrent requests to one shard and stalls on hung shards. On the shard, a batched write applies as ONE BadgerDB transaction — all-or-nothing per shard, matching the log-and-skip-per-shard failure model. Watch `badger.ErrTxnTooBig` only if the celebrity threshold ever rises well past 1000. Post success is returned once the post + author index are durably stored — fan-out outcome is observability, not the response
- **Error taxonomy (coordinator ↔ shard contract):** shard responses distinguish `missing` (200 with null/empty) from `shard-error` (5xx) from `down` (connection failure). Coordinator behavior per case: feed scan on shard-error → retry once via replica → else 502; celebrity fetch failure → skip that celebrity, merge the rest; fan-out write failure → log + skip (R7); any read error in the undetected-death window → one retry via replica. A fresh user's first feed read must return `200 []`, never 500
- **Write queue semantics:** bounded in-memory queue in the coordinator storing final key→value pairs (idempotent replay by construction). Eligibility is the contract: only writes whose KV pairs can be materialized offline without reading the down shard — A-side follow edges, unfollows, deletes. **Post creation is never queued** — the celebrity gate and fan-out pairs live on the down shard, so a queued post would drain silently missing its fan-out; posts to a down author's shard return 502. Overflow → 503, never silent drop; coordinator restart loses the queue — documented in `design.md`
- **Public user endpoint:** `GET /users/{user_id}` sits on the coordinator surface (not plan.txt's original surface, added so user data is readable without leaking key schema into handlers). Handlers and the client use semantic key helpers from the keys package — raw key assembly never appears outside it. Also gives the feed handler a clean existence check: unknown user → 404 "user not found" instead of a misleading empty feed
- **In-memory post→author map for `GET /posts/{post_id}`:** posts live on the author's shard, but post IDs are random and unroutable by entity ID. The coordinator keeps an in-memory `post_id → author_id` map populated at post creation and routes via the author; the map is lost on restart (lookups 404 loudly) — acceptable, documented in `design.md`
- **Replication contract:** absolute final values only (never operations — op replay would double-apply increments), deletes replicated, fire-and-forget from the primary's write path after local commit (no ack on the primary's write latency). **Unit of work is the transaction, not the key:** one primary transaction → one payload (its ENTIRE write-set — e.g. follower edge + incremented count together) → one replica transaction. Emitting per-key would let a failover window or overflow split the edge/count pair, violating the FollowerCount invariant and evading self-healing (a repeat follow is an idempotent no-op that never re-sends the count). **Ordering preserved by construction:** each replica gets one ordered channel drained by a single goroutine that POSTs in enqueue order — without it, per-write goroutines let two updates to the same key (count 5→6, then 6→7) arrive 7-then-6 and the replica settles wrong. Enqueue is non-blocking; on buffer overflow the NEWEST op is dropped and the replica stays stale on those keys until the next write to the same key re-sends a fresh absolute value (self-healing under traffic). The shard-side replicator is distinct from the coordinator write-queue (U10): replicator drops on overflow (stale replica — availability gap); write-queue must not drop (503 — loss-free rejection). Named distinctly in design.md
- **Deterministic merge + logical cursor:** sort by `(timestamp, post_id)`, dedupe by `post_id` (kills the threshold-crossing duplicate artifact). Pagination cursor is the logical `(ts19, post_id)` position, not a physical key — the merged page spans `feed:{user}:*` and celebrity `posts_by_author:*` on different shards, so each scan reconstructs its own prefix's seek key from the tuple and iterates backward with BadgerDB's seek-then-reset-prefix pattern
- **ID validation at the coordinator:** reject `:` and control characters in user IDs (a user named `bob` would lexicographically prefix-scan `bob:x`'s data — cross-user leakage), cap length at 64 chars. `POST /users` on an existing ID → 409, no overwrite (resetting FollowerCount would flip celebrity status; also keeps seed idempotency)
- **Celebrity check is O(following) live reads per feed load:** every followee's FollowerCount is read on each feed read (parallel). The tempting optimization — caching the celebrity flag in the following edge at follow time — is wrong: celebrity status changes after you follow, so the flag goes stale and the read path silently takes the wrong branch. Live reads are correct; the cost is deliberate and documented in design.md
- **Compression measured, not assumed:** zstd frames carry ~10+ bytes of overhead, so compressing tiny values (an Edge is two IDs; a FeedEntry is small) can inflate them. Decision at implementation: either skip compression below N bytes with a 1-byte raw/compressed flag, or compress everything and record the measured cost in design.md. Either is defensible — silently compressing 20-byte values is the one an interviewer catches
- **Latest-N reads via reverse iterator:** no pruning machinery; feed and author-index scans take the newest N entries directly

---

## Open Questions

### Resolved During Planning

- Timestamp padding width/epoch format (origin-deferred): nanoseconds, zero-padded to 19 chars, coordinator-stamped once per post
- Self-follow edge removable via unfollow API (origin-deferred): no — unfollow rejects `follower == followee` with 400
- Feed content shape: denormalized (see Key Technical Decisions)
- Replica mapping (plan.txt only wires `REPLICA` env on shards): coordinator gets a `REPLICAS` env (`shard1:replica1:8084,...`) so failover is implementable
- `plan.txt` hard contradiction (key schema has no author index): `plan.txt` amended in U1 to match R3/R4 and the `:{post_id}` key shape
- `GET /posts/{post_id}` routing (review finding, P1): coordinator in-memory post→author map; restart loss documented (see Key Technical Decisions)
- Post-to-down-author behavior (review finding, P1): 502, never queued; queue covers only offline-materializable writes (see Key Technical Decisions)
- `GET /users/{user_id}` on the coordinator surface (review finding, P3): added to U4 stub list and U6 wiring; key schema stays in the keys package (see Key Technical Decisions)
- Duplicate-follow semantics (review correction): idempotent conditional transaction — B-side checks the edge, writes it, and increments in ONE txn with bounded `badger.ErrConflict` retry; unfollow is conditional the other way (see Key Technical Decisions)
- Replication ordering (review correction): one ordered channel per replica drained by a single goroutine; drop-newest on overflow, self-healing via absolute values (see Key Technical Decisions)
- Unfollow residue (review correction): named gap, documented in design.md, not built (see Scope Boundaries)

### Deferred to Implementation

- [Msgpack library choice]: any maintained msgpack encoder (e.g., `vmihailenco/msgpack`) — swap freely; the storage wrapper isolates it
- [BadgerDB tuning]: value-log options, memtable size, `NumVersionsToKeep` — default options suffice; tune only if verification shows problems
- [Exact HTTP error codes and wording]: 400/404/502 conventions chosen during implementation, following the taxonomy contract
- [Frontend styling]: plain CSS, no design system — whatever makes the demo presentable
- [Coordinator queue size]: a sane default (e.g., 1024 entries) configurable via env; the 503 contract is the behavior, not the number
- [Compression threshold]: measured during U1 — skip-below-N-bytes with a 1-byte flag vs compress-all-with-recorded-cost, whichever the measurement supports (see Key Technical Decisions)
- [Replicator channel buffer]: generous default (a few thousand ops); overflow behavior (drop newest, self-heal) is the contract, not the number

---

## Output Structure

```
.
├── cmd/
│   ├── coordinator/main.go        # ./app coordinator entrypoint
│   └── shard/main.go              # ./app shard entrypoint (ROLE=replica uses same binary)
├── internal/
│   ├── hash/                      # consistent hash ring (hand-implemented, no library)
│   ├── storage/                   # BadgerDB wrapper + zstd compression
│   ├── models/                    # User, Post, FeedEntry, Edge structs + msgpack
│   ├── keys/                      # key encoding/decoding helpers (all key shapes)
│   ├── api/                       # HTTP handlers (shared client + shard + coordinator)
│   ├── fanout/                    # fan-out orchestration (write path + read merge)
│   └── replication/               # primary→replica payload logic
├── frontend/                      # React + Vite (3 views)
├── scripts/
│   ├── seed/                      # seed data generator (celebrity crossing)
│   └── demo/                      # demo + resilience verification scripts
├── Dockerfile                     # single binary `./app` for all 7 services
├── docker-compose.yml             # coordinator + shard1-3 + replica1-3
├── design.md                      # the interview deliverable (written per-unit)
├── docs/notes/                     # per-unit learning journal (README index + 01..06 phase files)
├── go.mod                          # module `socialfeed`
├── plan.txt                       # amended in U1 (key schema + read-path step 4)
└── README.md                      # Phase 6 polish
```

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

```mermaid
flowchart TB
    subgraph WRITE["Post creation (F1)"]
        P0[User posts] --> P1[Coordinator stamps id + ns timestamp]
        P1 --> P2[Store Post + author-index entry on author's shard]
        P2 --> P3{Author follower count < threshold?}
        P3 -->|no - celebrity| P4[Done - lazy delivery on read]
        P3 -->|yes| P5[Fetch follower IDs - range scan on author's shard]
        P5 --> P6[Group followers by shard - batched FeedEntry writes]
        P6 --> P7[Down shard? log + skip - post still returns success]
    end
    subgraph READ["Feed read (F2)"]
        R0[Open feed] --> R1[Reverse scan own feed entries - denormalized content]
        R1 --> R2[Fetch followee list on own shard]
        R2 --> R3[Filter celebrities by follower count]
        R3 --> R4[Scatter-gather celebrity posts via author index]
        R4 --> R5[Merge + sort (ts, post_id) + dedupe by post_id]
        R5 --> R6[Return top N, cursor for pagination]
    end
```

---

## Implementation Units

### Phase 1 — Single node works

### U1. Project scaffolding, storage layer, plan.txt amendment

**Goal:** Go module, key encoding helpers, BadgerDB+zstd storage wrapper, and a corrected `plan.txt` key schema — the foundation every other unit builds on.

**Requirements:** R1, R3

**Dependencies:** None

**Files:**
- Create: `go.mod`, `internal/keys/keys.go`, `internal/keys/keys_test.go`, `internal/storage/storage.go`, `internal/storage/storage_test.go`, `internal/models/models.go`, `internal/models/models_test.go`, `design.md` (skeleton + key-design section per R10)
- Create (already scaffolded): `docs/notes/` (README index + `01-single-node.md`..`06-polish.md` templates) — the per-unit learning journal; each subsequent unit fills its phase file's sections and updates the README status, alongside `design.md`
- Modify: `plan.txt` (key schema section: add `posts_by_author:{author_id}:{ts19}:{post_id}`, change feed/author-index key shapes to include the post-ID suffix; read-path step 4 → "via the author index")

**Approach:**
- Module name `socialfeed`; stdlib only for IDs (random hex via `crypto/rand`) — no external ID library
- Key shapes, all zero-padded 19-char ns timestamps: `users:{id}`, `posts:{id}`, `posts_by_author:{author}:{ts19}:{post_id}`, `feed:{user}:{ts19}:{post_id}`, `graph:followers:{user}:{follower}`, `graph:following:{user}:{followee}`
- Key encoding helpers own every key shape (encode + decode) so no other package hand-assembles keys
- Storage wrapper: open/close with `badger.DefaultOptions`, `Get` (returns a `missing` sentinel), `Set` (msgpack-encode + zstd-compress values), `Delete`, `Scan(prefix, limit, reverse)` — wrapper isolates BadgerDB v4 and the codec from callers. Compression decision made during this unit: measure zstd vs raw on real Edge/FeedEntry sizes; either skip compression below N bytes with a 1-byte raw/compressed flag, or compress everything and record the measured cost in `design.md` (see Key Technical Decisions)
- Write `design.md` skeleton now; per-unit sections accumulate (R10)

**Test scenarios:**
- Happy path: Set→Get roundtrip preserves bytes exactly (msgpack + zstd decode is lossless)
- Happy path: Scan over a prefix returns entries in lexicographic order; reverse scan returns newest-first
- Edge case: Get on absent key returns the `missing` sentinel, not an error
- Edge case: Scan with an empty range returns an empty result, not an error
- Edge case: key encoding roundtrip for every shape — encode then decode reproduces the components; timestamp padding is exactly 19 chars and sorts chronologically; post IDs are fixed-width
- Edge case: measured sizes — compressed vs raw for typical Edge/FeedEntry values; the raw/compressed flag (if adopted) roundtrips
- Error path: operations on a closed DB return an error instead of panicking
- Integration: a compressed value larger than the LSM inline threshold roundtrips through the value log

**Verification:** All storage and key tests pass; `plan.txt` key schema matches the keys in `internal/keys`; `design.md` has a key-design section.

---

### U2. Shard node HTTP server + binary entrypoints

**Goal:** A working single-node shard: HTTP API (`/write /read /scan /delete /health`, plus the internal batch-write and count-bearing follow-edge additions) backed by the storage wrapper, runnable via `./app shard`.

**Requirements:** R1

**Dependencies:** U1

**Files:**
- Create: `cmd/shard/main.go`, `internal/api/shard.go`, `internal/api/shard_test.go`
- Modify: `internal/storage/storage.go` (batch write primitive; `AddFollower`/`RemoveFollower` conditional transactions), `internal/storage/storage_test.go` (the conditional-transaction tests)

**Approach:**
- Shard API surface: `POST /write` (single or batch of final key→value pairs), `GET /read?key=`, `GET /scan?prefix=&limit=&reverse=`, `DELETE /delete?key=`, `POST /follow-edge`, `POST /unfollow-edge`, `GET /health` (node ID, role, stats), `POST /replicate` (Phase 5; stub a 501 here or wire in U9)
- **The conditional transaction lives in the storage package, not the API handler** (codec-isolation invariant): `storage.AddFollower(followee, follower)` / `storage.RemoveFollower(...)` do the check-write-increment (or check-delete-decrement) in ONE transaction with the bounded `badger.ErrConflict` retry loop, doing the decode/increment/re-encode against the raw txn inside the wrapper where the codec already lives. The `POST /follow-edge` / `POST /unfollow-edge` HTTP handlers are thin calls into those methods. Keeping this in `api/shard.go` would leak zstd/msgpack across the wrapper boundary, breaking "the wrapper isolates the codec from callers"
- The count-bearing transaction replaces the generic increment idea. A generic increment endpoint cannot back the follow path (edge + count as two transactions can drift apart between them). Invariant: FollowerCount always equals the number of real follower edges, excluding the self-edge
- BadgerDB optimistic concurrency: concurrent writes to the same user record commit-fail with `badger.ErrConflict`, and `db.Update` does not retry. The follow-edge methods retry conflicting transactions a bounded number of times — this is what makes concurrent follows converge (the U6 100-goroutine test is testing conflict handling, not Badger atomicity)
- Response contract per the error taxonomy: missing → 200 with `{"value": null}` / `{"entries": []}`; malformed request → 400; internal → 500
- Entrypoints follow plan.txt's single-binary contract: `./app shard` reads `NODE_ID`, `PORT`, `ROLE`, `REPLICA` env
- `design.md`: storage-engine + key-design sections get their "why" written here (R10)

**Test scenarios:**
- Happy path: write→read→scan→delete roundtrip over HTTP
- Happy path: batch write persists every pair; scan returns all of them
- Happy path: `POST /follow-edge` on a fresh edge writes the edge and increments the count in one transaction
- Happy path: repeated `POST /follow-edge` for the same pair is idempotent — edge exists → no write, no increment, count unchanged
- Edge case: scan with `limit` returns at most N; `reverse=true` returns newest first
- Edge case: `POST /unfollow-edge` for a pair with no edge is a no-op (no delete, no decrement)
- Edge case: concurrent `POST /follow-edge` calls to the same followee converge on the correct final count (bounded conflict retry)
- Error path: malformed JSON → 400 with an error body; unknown route → 404
- Error path: read of absent key → 200 with null value (the `missing` case — the coordinator depends on this distinction)
- Integration: full HTTP roundtrip against a real opened DB in `httptest` (no mocks for the storage layer)

**Verification:** Shard API tests pass; `./app shard` boots with env config and answers `/health`; `design.md` storage section written.

---

### Phase 2 — Cluster works

### U3. Consistent hash ring

**Goal:** Hand-implemented consistent hash ring with virtual nodes — no library — as the coordinator's routing primitive.

**Requirements:** R1

**Dependencies:** U1

**Files:**
- Create: `internal/hash/ring.go`, `internal/hash/ring_test.go`
- Modify: `design.md` (hashing section per R10)

**Approach:**
- Ring per plan.txt: sorted virtual-node hash positions, `AddNode`/`RemoveNode`/`GetNode` (binary search, clockwise)
- 150 virtual nodes per physical node; deterministic hashing so tests are stable
- `RemoveNode` implemented and tested (API completeness per plan.txt) but never used for failure handling — static membership is a stated invariant (see Key Technical Decisions)

**Execution note:** Implement test-first — write the failing tests for deterministic routing and even distribution before the ring.

**Test scenarios:**
- Happy path: `GetNode` returns the same node for the same key across calls (determinism)
- Happy path: with 3 nodes, keys distribute across all three; none returns an empty share with 150 virtual nodes
- Edge case: empty ring → `GetNode` returns a clear error
- Edge case: single node → every key maps to it
- Edge case: `RemoveNode` → its keys re-map to the remaining nodes; remaining nodes' keys are untouched (consistency under removal)
- Edge case: keys that hash exactly onto a virtual-node boundary resolve correctly
- Integration: 10,000 random keys across 3 nodes stay within a sane distribution bound (e.g., no node below ~25%)

**Verification:** All ring tests pass; distribution test is stable across runs; `design.md` hashing section written.

---

### U4. Coordinator routing + cluster health

**Goal:** Coordinator binary that owns the ring, routes requests to shards by entity ID, and exposes `/health` and `/cluster`.

**Requirements:** R1

**Dependencies:** U2, U3

**Files:**
- Create: `cmd/coordinator/main.go`, `internal/api/coordinator.go`, `internal/api/coordinator_test.go`, `internal/api/client.go` (shard HTTP client)
- Modify: `design.md` (routing section per R10)

**Approach:**
- Env contract: `SHARDS=shard1:8081,shard2:8082,shard3:8083`, `REPLICAS=shard1:replica1:8084,...`, `CELEBRITY_THRESHOLD` (default 1000)
- Router helper: `ShardFor(entityID)` → ring lookup — the single place routing is decided; entity-ID-only hashing invariant lives here
- Client wrapper around the shard API: distinguishes `missing` / `shard-error` / `down` (timeout → `down`), supports the batch endpoint
- `/cluster` reports per-node: ID, role, reachable/healthy from the latest heartbeat pass (heartbeat stub in U10 — report configured nodes now)
- Public coordinator API from plan.txt is routed but the handlers for users/posts/feed/follow land in later units — wire them as 501 stubs here so the surface exists. Stub list includes `GET /users/{user_id}` (wired in U6); the full public surface is: `POST /users`, `GET /users/{user_id}`, `POST /follow`, `DELETE /follow`, `POST /posts`, `GET /posts/{post_id}`, `GET /feed/{user_id}`

**Test scenarios:**
- Happy path: `ShardFor` routes the same entity ID to the same shard consistently
- Happy path: requests to a live fake shard (httptest) succeed end-to-end through the client
- Edge case: `ShardFor` never returns a removed node's shard (post-removal keys map elsewhere)
- Error path: client classifies connection-refused and timeout as `down`; 5xx as `shard-error`; 200+null as `missing`
- Error path: an unconfigured node ID in `SHARDS` env fails startup with a clear message
- Integration: `/cluster` lists all configured shards with their roles

**Verification:** Coordinator tests pass; `./app coordinator` boots, answers `/health`, and `/cluster` lists the configured topology; `design.md` routing section written.

---

### U5. Docker Compose cluster

**Goal:** The 7-service cluster from plan.txt runs under Compose, and writes land on the correct shards.

**Requirements:** R1

**Dependencies:** U2, U4

**Files:**
- Create: `Dockerfile` (single-stage Go build producing `/app`; frontend served separately or deferred to U11), `docker-compose.yml` (coordinator + 3 shards + 3 replicas, named volumes at `/data`, env per plan.txt plus `REPLICAS` on the coordinator)
- Modify: `design.md` (deployment/topology note per R10)

**Approach:**
- One binary, one Dockerfile; `ROLE=replica` env makes replicas listen on their own port (same `./app shard` binary)
- Coordinator `SHARDS`/`REPLICAS` point at service names inside the Compose network
- Verification is manual/scripted against the live cluster (no test files — infrastructure)

**Test scenarios:**
- Integration: `docker compose up` brings up all 7 containers healthy
- Integration: write a key through the coordinator's shard-facing path and verify it lands on the ring-correct shard (check the shard's `/health` or a direct `/read`)
- Integration: restart a shard container; its volume survives; reads still work
- Error path: `docker compose config` validates; a container with a missing `NODE_ID` fails fast with a clear log line

**Verification:** Cluster comes up; routing verified against the live topology; topology note added to `design.md`.

---

### Phase 3 — Social graph works

### U6. Users + follow/unfollow

**Goal:** User creation with self-follow, and cross-shard follow/unfollow with the conditional-transaction contract.

**Requirements:** R3 (key discipline), R5, R6 (self-edges), R8, R9; origin flow F3

**Dependencies:** U2 (shard batch + follow-edge API), U4

**Files:**
- Create: `internal/api/users.go`, `internal/api/users_test.go`, `internal/fanout/graph.go` (edge write orchestration), `internal/fanout/graph_test.go`
- Modify: `internal/api/coordinator.go` (wire `POST /users`, `GET /users/{user_id}`, `/follow`, `/unfollow` handlers), `design.md` (graph + social section per R10)

**Approach:**
- `POST /users`: validate ID (reject `:` and control chars, cap 64) → build user record → route to `ShardFor(user)` → one shard-local transaction writing the user record + both self-edges (`graph:following:{u}:{u}`, `graph:followers:{u}:{u}`) — FollowerCount starts at 0. An existing ID → 409, no overwrite (re-creating a user would reset FollowerCount to 0 and flip a celebrity back to normal; also keeps U12 seed idempotency)
- `GET /users/{user_id}`: public coordinator route returning the user record (the only read of user data the coordinator surface exposes). Handler validates the ID, routes via `ShardFor(user_id)`, and reads through the client using the keys package's user-key helper — key assembly never appears in handlers
- `POST /follow` `{follower, followee}`: reject `follower == followee` (400); **validate both IDs resolve to existing users → 404, nothing written** (the B-side `AddFollower` surfaces `ErrKeyNotFound` when the followee doesn't exist; the A-side must not silently create a dangling following edge for a non-existent follower); A-side write (`graph:following:{follower}:{followee}`) on the follower's shard — a plain Set, naturally idempotent (no count rides on it); B-side via `POST /follow-edge` on the followee's shard — conditional edge write + count increment in one transaction, retried on `badger.ErrConflict` (see U2). The narrowed half-edge contract: a partial failure leaves only the A-side edge missing (benign miss), never a drifted count
- `DELETE /follow` (or `POST /unfollow` if tooling awkwardness wins — pick at implementation): reverse of follow; A-side plain Delete; B-side via `POST /unfollow-edge` — conditional (edge absent → no-op, no decrement)
- Cross-shard failure: best effort both sides, log the outcome — the accepted half-edge gap (R8), now with the count guaranteed consistent with the B-side edge
- `design.md`: write the cross-shard write + CAP section here (R10)

**Test scenarios:**
- Happy path: user creation writes the user record and both self-edges in one transaction (verify via scans)
- Happy path: `GET /users/{id}` returns the stored record with its FollowerCount; unknown user → 404 (distinct from the shard-level `missing` — the coordinator maps it)
- Happy path: follow writes both edges and increments the count exactly once
- Happy path: unfollow removes both edges and decrements the count
- Edge case: follow self → 400, nothing written
- Edge case: follow of a non-existent followee → 404, nothing written (B-side `AddFollower` gets `ErrKeyNotFound`)
- Edge case: follow by a non-existent follower → 404, nothing written (no dangling following edge created)
- Edge case: duplicate follow → idempotent no-op: edge already exists, count unchanged (FollowerCount is the celebrity gate — a blind increment would drift the count away from the real edge set and decide fan-out mode on a meaningless number)
- Edge case: unfollow when not following → idempotent no-op (no decrement — drifting the other way is equally wrong)
- Edge case: double follow then unfollow → count returns exactly to baseline (no permanent miscount)
- Edge case: ID with `:` → 400; ID over 64 chars → 400
- Error path: followee shard down → A-side edge written, B-side failed, count unchanged, error surfaced (the accepted half-edge, observable in logs)
- Integration: concurrent follows of the same followee (100 goroutines) → final count equals the number of unique followers (the bounded conflict-retry loop converges; without it ~99 would fail the BadgerDB commit race)
- Integration: user → follow → graph scans show both directions on the correct shards (routing-by-entity-ID honored)

**Verification:** Graph tests pass; concurrent-follow test shows no lost increments; `design.md` graph section written; a manual curl flow (create user → self-follow visible in both edge scans) works against the Compose cluster.

---

### Phase 4 — Fan-out works

### U7. Post creation + fan-out on write

**Goal:** The write path: coordinator-stamped post + author index + batched FeedEntry fan-out with the celebrity gate.

**Requirements:** R1, R3, R4 (index written), R6, R7, R9; origin flow F1, origin AEs AE1, AE2, AE4

**Dependencies:** U3 (routing), U4 (coordinator wiring), U5 (cluster), U6 (follower graph reads)

**Files:**
- Create: `internal/fanout/writer.go` (post materialization + fan-out), `internal/fanout/writer_test.go`
- Modify: `internal/api/coordinator.go` (wire `POST /posts`, `GET /posts/{id}`), `design.md` (fan-out section per R10)

**Approach:**
- Coordinator generates `post_id` (fixed-width `crypto/rand` hex — the same lexicographic-sort invariant as zero-padded timestamps) and one ns timestamp, materializes all KV pairs first: `posts:{id}` and `posts_by_author:{author}:{ts}:{id}` on the author's shard; per-follower `feed:{follower}:{ts}:{id}` (denormalized content — PostID, AuthorID, Timestamp, Content) on each follower's shard
- `GET /posts/{post_id}` routing: posts live on the author's shard but the endpoint receives only a random post_id — the coordinator keeps an in-memory `post_id → author_id` map, populated when it stamps each post at creation, and routes via `ShardFor(author)`. The map is lost on coordinator restart; lookups for pre-restart posts 404 loudly — acceptable (the demo never reads posts by ID), documented in `design.md`
- Celebrity gate: read author's FollowerCount; below threshold → fan out; at/above → store post + index only (lazy read delivery)
- Fan-out execution: group followers by `ShardFor(follower)`; one batched write call per shard with a short deadline; each shard applies its batch as ONE transaction (all-or-nothing per shard — matches the log-and-skip-per-shard failure model); failures logged and skipped (R7); post response returns once post + index are durably stored, never waiting on fan-out
- The self-follow edges guarantee own-post delivery in both modes (non-celebrity: fan-out includes self; celebrity: read path picks self up — AE1, AE2)
- `design.md`: write-path section including the fan-out/Kafka narrative (R10)

**Test scenarios:**
- Happy path: non-celebrity post → post + author index stored on author's shard, one FeedEntry per follower on the correct shards
- Happy path: post content appears verbatim in the follower's feed entry (denormalization)
- Edge case: author with zero followers → post + index only, no fan-out writes, 200
- Edge case: follower count exactly at the threshold boundary — 999 → fan-out runs; 1000 → lazy (test both sides with a controlled count)
- Edge case: celebrity post → zero fan-out writes; `posts_by_author` scan on the author's shard finds it (AE2 groundwork)
- Error path: one destination shard down → its entries skipped and logged, post still returns 200, other shards' entries written (AE4)
- Error path: author's shard down → post fails with 502 and is never queued (the celebrity gate and fan-out pairs cannot be materialized offline — see U10; the queue only ever holds offline-materializable writes)
- Edge case: `GET /posts/{id}` for a post created before a coordinator restart → 404 (in-memory map lost, documented in `design.md`); for a live post → returns the post from the author's shard
- Integration: post → every follower's feed scan on their shard contains exactly one entry with the correct post ID

**Verification:** Writer tests pass including both threshold sides and the AE4 partial-loss case; `design.md` fan-out section written.

---

### U8. Feed read path

**Goal:** The read path: merged chronological feed with celebrity lazy delivery, deterministic ordering, dedupe, and cursor pagination.

**Requirements:** R1, R3, R4, R6; origin flow F2, origin AEs AE1, AE2, AE3

**Dependencies:** U6 (followee graph), U7 (feed entries exist)

**Files:**
- Create: `internal/fanout/reader.go` (merge + dedupe + cursor), `internal/fanout/reader_test.go`
- Modify: `internal/api/coordinator.go` (wire `GET /feed/{user_id}?limit=&before=`), `design.md` (read-path + merge section per R10)

**Approach:**
- Step 1: reverse scan `feed:{user}:*` on the user's shard (latest N, denormalized content — no post fetches)
- Step 2: fetch `graph:following:{user}:*` on the user's shard; read FollowerCount for each followee via the coordinator user endpoint (or its client helper — never raw key reads in handlers; key assembly stays in the keys package), parallel across followees
- Celebrity detection is O(following) live count reads per feed load — deliberately paid, not cached (status changes after follow; a stale cached flag would silently take the wrong read path). The cost and the rejected optimization are documented in `design.md`
- Step 3: celebrities (count >= threshold) → reverse scan `posts_by_author:{celeb}:*` on their shards (one scan per celebrity, concurrent across shards, short deadline)
- Step 4: normalize both sources to one entry shape; merge; sort by `(timestamp, post_id)`; dedupe by `post_id` (kills threshold-crossing duplicates); apply `limit`; return entries + a `next_cursor` when more exist
- **The cursor is LOGICAL, not a physical BadgerDB key.** A merged page draws from two prefixes — `feed:{user}:*` and each celebrity's `posts_by_author:{celeb}:*` — on possibly different shards, so no single physical key can seek into both. The cursor is the logical `(ts19, post_id)` position; on the next page each underlying scan reconstructs its own prefix's seek key from that tuple (`feed:{user}:{ts}:{pid}` for the feed scan, `posts_by_author:{celeb}:{ts}:{pid}` for each celebrity scan) and iterates backward from there, using the same seek-then-reset-prefix mechanic. Portable and prefix-independent. A physical-key cursor would silently work with no celebrities and then drop or repeat celebrity posts across page boundaries once one is in the mix — exactly the demo path
- Error handling: celebrity fetch failure → skip that celebrity and merge the rest; user's own shard failure → one retry via replica, then 502
- Unknown user: the feed handler checks user existence via `GET /users/{user_id}` (routed, then user key read on the shard) before scanning; missing record → 404, so a mistyped ID renders "user not found", never a misleading empty feed
- Empty feed → `200 []` with no cursor — the most likely first demo screen (see origin: F2)
- `design.md`: read-path + scatter-gather section (R10)

**Test scenarios:**
- Happy path: non-celebrity author's post appears in follower's feed via fan-out entries (AE1 variant)
- Happy path: celebrity author's post appears in follower's feed via the author index with zero fan-out entries (AE3)
- Happy path: celebrity's own feed contains own posts via the read path with zero fan-out writes (AE2)
- Happy path: mixed feed of normal + celebrity followees merges in correct timestamp order
- Edge case: empty feed → `200 []`, no error
- Edge case: timestamp ties → order is deterministic across repeated reads (secondary post_id sort)
- Edge case: threshold-crossing duplicate — same post present via both paths → appears exactly once (dedupe)
- Edge case: `limit` + `before` cursor walks the whole feed without gaps or repeats (pagination across page boundaries)
- Edge case: pagination crossing a page boundary WITH a celebrity in the followee set — no celebrity post is dropped or repeated across pages (logical-cursor test; the merged page spans two key prefixes)
- Error path: one celebrity's shard down → feed returns the remaining entries, no error
- Error path: user's own shard down → one replica retry, then 502
- Integration: end-to-end — create users, follow, post on both sides of the threshold, read feed, verify content + order

**Verification:** Reader tests pass including AE1/AE2/AE3 shapes and pagination walks; `design.md` read-path section written.

---

### Phase 5 — Resilience works

### U9. Replication

**Goal:** Async primary→replica replication of absolute final values, fire-and-forget from the shard write path.

**Requirements:** R1, R9 (async-only, no consensus)

**Dependencies:** U2 (shard server), U5 (replica containers exist)

**Files:**
- Create: `internal/replication/replicator.go`, `internal/replication/replicator_test.go`
- Modify: `internal/api/shard.go` (wire `POST /replicate`), `internal/storage/storage.go` (expose a write hook if needed), `design.md` (replication + CAP section per R10)

**Approach:**
- **Unit of work is the transaction, not the key:** one primary transaction → one replication payload → one replica transaction. The write hook must group a transaction's ENTIRE write-set (edge + incremented count) into a single op — never emit one payload per key. Per-key emission would split the pair: during a failover window the replica could show the follower edge present but the count not yet bumped (a live violation of the FollowerCount invariant), and on overflow it could drop the count payload but keep the edge, leaving the replica split until both keys are rewritten — which a repeat follow (idempotent no-op on the primary) would never heal. Grouping them as one payload means they are applied atomically on the replica and dropped/kept as a unit
- Primary write path: after the local transaction commits, asynchronously enqueue the transaction's whole write-set (key→value pairs and/or deletes) as one op for the replica — one ordered channel per replica, drained by a single goroutine that POSTs in enqueue order. Per-key order by construction: per-write goroutines would let two updates to the same key (count 5→6, then 6→7) arrive 7-then-6 and leave the replica settled wrong. Non-blocking enqueue (replication never sits on the primary's write latency); buffer overflow drops the NEWEST op and the replica self-heals on the next write to that key (absolute values re-send the truth). No ack, no retry; failures logged
- `ROLE=replica` must NOT construct a replicator — same binary, so it is a startup branch; a replica replicating onward would hammer a non-existent chain
- Naming: this shard-side replicator is distinct from the coordinator write-queue (U10) — replicator drops on overflow (stale replica, availability gap); write-queue never drops (503, loss-free rejection). Named distinctly in design.md
- `/replicate` payloads are absolute final values only — never operations (increment replay would double-apply; the primary sends the final computed value). A payload carries one transaction's full write-set (see unit-of-work contract above)
- Deletes replicate too; each payload applies on the replica as one transaction (the replica side of the one-txn→one-op→one-txn contract)
- Replica is a full independent BadgerDB instance; no catch-up/backlog on missed replication — accepted gap (R9), documented in `design.md`
- `design.md`: replication section — write the CAP position and the "replica serves stale reads" talking point (R10)

**Test scenarios:**
- Happy path: write to primary → payload arrives at the replica with identical key→value bytes (final value, not op)
- Happy path: delete on primary → delete arrives at the replica
- Happy path: two increments → replica receives the final values, not the ops (values not double-applied)
- Edge case: a follow-edge write (edge + incremented count) arrives as ONE payload and is applied atomically on the replica — the replica never shows an edge without its matching count (transaction-unit contract; asserts the write hook groups the whole write-set, not per-key)
- Edge case: two rapid updates to the same key (count 5→6, then 6→7) arrive in order — replica settles on 7 (single-drain ordering, asserted via a sequenced harness, not a timing test)
- Edge case: replica down → primary keeps serving, failure logged, write latency unaffected (fire-and-forget verified by timing)
- Edge case: buffer overflow under sustained outage → newest ops dropped with a log line; after recovery, a fresh write to the same keys heals the replica (self-healing)
- Error path: malformed replication payload → 400 on the replica, primary unaffected
- Integration: failover drill — primary stopped, reads served from replica reflect the last replicated state (stale-after-outage window is the documented gap)

**Verification:** Replication tests pass; `design.md` replication section written; failover drill against Compose shows replica reads.

---

### U10. Heartbeat, failure detection, failover, write queue

**Goal:** Coordinator detects down nodes, routes reads to replicas, and queues writes to down primaries with idempotent replay.

**Requirements:** R1, R7 (no backfill for fan-out), R9 (gaps documented, not fixed)

**Dependencies:** U4 (topology + client), U5 (Compose cluster for drills), U9 (replicas functional)

**Files:**
- Create: `internal/api/heartbeat.go` (monitor loop), `internal/api/heartbeat_test.go`, `internal/api/writequeue.go` (bounded queue), `internal/api/writequeue_test.go`
- Modify: `internal/api/coordinator.go` (failover routing + queue integration), `internal/api/client.go` (replica-aware routing helper), `design.md` (failure-handling section per R10)

**Approach:**
- Heartbeat loop: ping every node every 5s; 3 consecutive misses → `down`; recovery detected by a successful ping → `up`, drain queue
- Read failover: reads to a `down` primary route to its replica; reads during the undetected-death window get one retry via replica on `shard-error`/`down` (the 15s window behavior is defined, not incidental)
- Write queue: bounded in-memory queue per down primary storing final key→value pairs (idempotent replay — all pairs were materialized before queueing); drain on recovery; overflow → 503 (never silent drop); coordinator restart loses the queue — documented in `design.md`
- **Queue eligibility — post creation is never queued:** a post to a down author's shard returns 502 immediately. The celebrity gate (FollowerCount) and the fan-out pairs live on the down shard, so a queued post could only drain as post+index — silently losing fan-out beyond R7's accepted scope, which covers destination-shard loss only. The queue holds only writes whose KV pairs are materializable offline without reading the down shard: A-side follow edges, unfollows, and deletes (U7's contract governs the post path)
- Fan-out writes to a down shard stay log-and-skip per R7 — they are NOT queued (feed entries are best-effort by decision; the queue is only for offline-materializable direct user-facing writes)
- `design.md`: failure-handling section — heartbeat, failover, the 15s window, queue semantics, split-brain acknowledgment (R10)

**Test scenarios:**
- Happy path: 3 missed pings → node flips to `down` in `/cluster`
- Happy path: a down primary's read routes to its replica; recovery → reads route back to the primary
- Happy path: writes to a down primary queue; on recovery the queue drains and the final state matches (idempotent replay — A-side follow edges and deletes applied exactly once, no duplicates)
- Edge case: post to a down primary → 502, nothing queued (post creation is queue-ineligible by contract)
- Edge case: queue overflow → 503 with a clear error, queue contents intact
- Edge case: a node flapping between up/down doesn't corrupt the queue (drain exactly once)
- Error path: read during undetected-death window → one replica retry before surfacing an error
- Integration: Compose drill — `docker stop shard2`, wait ~15s, verify `/cluster` flip, replica-served reads, queued writes draining after `docker start shard2`

**Verification:** Heartbeat/queue tests pass; the Compose resilience drill succeeds end-to-end; `design.md` failure section written.

---

### Phase 6 — Polish

### U11. React frontend (3 views)

**Goal:** Feed View, Post View, Follow View — plain React with `useState` + fetch, no auth, no state library.

**Requirements:** R11

**Dependencies:** U7, U8 (feed + post APIs), U6 (follow API)

**Files:**
- Create: `frontend/` (Vite scaffold: `package.json`, `index.html`, `src/main.jsx`, `src/App.jsx`, `src/views/FeedView.jsx`, `src/views/PostView.jsx`, `src/views/FollowView.jsx`)
- Modify: `docker-compose.yml` (frontend service or static-serve instruction)

**Approach:**
- Exactly the three views from plan.txt; simple tab/nav between them; plain `useState` + `fetch`; no router library (one component switching on state suffices)
- Feed View: enter user ID → show merged feed with limit + load-more (cursor); Post View: user ID + content → `POST /posts` → success confirmation; Follow View: follower/followee inputs, follow/unfollow buttons, current following list
- Empty feed renders as "no posts yet" — not an error (the taxonomy contract surfaces as UX)
- Vite dev proxy to `localhost:8080` for local dev; production build served as a static site in Compose

**Test scenarios:**
- Happy path: Feed View renders a seeded user's merged feed, newest first, with post content visible (denormalized entries render without extra fetches)
- Happy path: Feed View on an empty user renders "no posts yet" — not an error (the `200 []` taxonomy contract surfaces as UX)
- Happy path: Post View submits a post and shows success; the post appears in the author's feed on reload (self-follow paths)
- Happy path: Follow View follows/unfollows and the following list updates
- Edge case: Feed View load-more walks pages via the cursor without duplicates
- Error path: cluster down → views surface a readable error instead of hanging silently (client timeout handling)
- Integration: U12 demo script exercises all three views against the running cluster

**Verification:** All three views work against the running Compose cluster; empty feed renders gracefully; post and follow roundtrips show correct state.

---

### U12. Seed data + demo verification

**Goal:** A deterministic seed script (with a celebrity crossing) and a verification script that proves the demo claims.

**Requirements:** R12; origin AEs AE1–AE4 observable in the demo

**Dependencies:** U6, U7, U8, U10

**Files:**
- Create: `scripts/seed/main.go` (or shell script — pick at implementation), `scripts/demo/verify.sh` (or Go verification binary), `README.md` (demo runbook)
- Modify: `design.md` (final assembly + "what breaks at scale" + "what you'd do differently" per R10)

**Approach:**
- Seed order matters: create users → follow → then post (no backfill; post-then-follow yields empty feeds — the demo confusion to avoid, see origin: F2)
- Seed shape: a small social graph (e.g., alice/bob/carol + 1000-follower celebrity `celeb`) where `celeb` crosses the threshold with real followers, then posts — lazy delivery observable via coordinator fan-out logs
- Space seeded posts beyond the timestamp granularity and verify distinct post IDs in feeds (the `:{post_id}` suffix protects anyway)
- Verification script asserts: both fan-out modes observable (fan-out log lines for normal posts, none for celebrity), celebrity crossing at exactly 1000 real followers, own-posts-in-feed for both a normal user and the celebrity (AE1, AE2), follower sees celebrity post with zero fan-out (AE3), and a resilience run (AE4 + U10 drill)
- `README.md`: runbook — prerequisites, up, seed, demo script, resilience drill
- `design.md` final assembly (R10): the interview-ready document per plan.txt's day-19 structure, written fresh against the build

**Test scenarios:**
- Happy path: seed completes; every seeded user's feed is non-empty and correctly ordered
- Happy path: celebrity post produces zero fan-out log lines and appears in a follower's feed
- Edge case: re-running seed is idempotent or explicitly fails fast (no silent duplicate users)
- Error path: seed against a partially-down cluster reports exactly which steps failed
- Integration: full demo run from a clean `docker compose up` to verified feeds in one script

**Verification:** `verify.sh` exits 0 against a fresh cluster; demo is repeatable; `README.md` runbook works from scratch; `design.md` is complete.

---

### U13. Design doc review pass (interview readiness)

**Goal:** A final pass over `design.md` for interview readiness — the real deliverable (see origin: Problem Frame, A2).

**Requirements:** R9 (gaps documented), R10

**Dependencies:** U12

**Files:**
- Modify: `design.md`

**Approach:**
- Read `design.md` as the interviewer would: every key decision must have a why, every accepted gap a production answer, every claim a "what breaks at scale" consequence
- Verify the sections from plan.txt's design-doc structure are all present: problem, architecture, key decisions (hashing, BadgerDB, hybrid fan-out, threshold placement), tradeoffs (CAP position, consistency vs availability), what breaks at scale, what you'd do differently (Raft, Kafka, Redis, gRPC)
- Dry-run the threshold-crossing and failover narratives aloud — the Phase 4 / Phase 5 demo stories (crossing 999→1000 both directions; the 15s detection window; stale replica reads; post-loss during outage as the Kafka story)

**Test scenarios:**
- Test expectation: none — document review; verification is the checklist below

**Verification:** Every section of the plan.txt design-doc structure is present and non-empty; a reviewer (the user, or a practice interview partner) can extract the threshold and failover narratives from the document alone.

---

## System-Wide Impact

- **Interaction graph:** Every coordinator handler talks to shards through the shared client — the error taxonomy (missing/shard-error/down) and entity-ID routing are load-bearing for all of U4–U10; changes to the shard API ripple through the client and every coordinator handler
- **Error propagation:** Shard `missing` is not an error (200, null/empty). `shard-error` retries once via replica for reads (feed/celebrity paths) and is logged-and-skipped for fan-out; `down` gates queueing for offline-materializable user-facing writes (A-side follow edges, unfollows, deletes) and fails post creation with 502 — never a silently accepted post. The 503 queue-overflow response is the only acknowledged-loss-free rejection
- **State lifecycle risks:** Cross-shard follow writes leave half-edges by design (R8) — the A-side edge may exist without the B-side; the conditional-transaction contract keeps counts consistent with B-side edges. Fan-out loss is permanent by design (R7). Queue loss on coordinator restart is acknowledged. Post-ID-suffix keys prevent collision overwrites
- **API surface parity:** plan.txt's public coordinator API is honored exactly; the internal shard API gains batch-write, count-bearing follow-edge/unfollow-edge, and replicate endpoints — internal surface only, documented in `design.md`
- **Integration coverage:** The Compose cluster is the integration surface — every phase's verification runs against real containers (routing correctness, failover drill, seed + verify). Unit tests use `httptest` with real BadgerDB instances, not mocks
- **Unchanged invariants:** No auth, no consensus, no sync replication, no feed cache — the accepted-gap list (R9) stays intact throughout; the plan adds no machinery that silently upgrades any of them

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| BadgerDB v4 API drift (plan was written against verified v4 patterns) | Storage wrapper isolates the DB; implementation verifies against installed v4 before Phase 1 closes |
| Msgpack library choice unresolved | Wrapper isolates the codec; any maintained encoder works |
| WSL filesystem + Docker networking quirks (host ports, `localhost` vs container names) | Compose verification in U5 and U12 exercises the full path early; README runbook captures working commands |
| npm `.npmrc` prefix warning breaks Vite scaffold | Benign warning verified present; if `npm create vite` fails, scaffold manually |
| Seed loop timestamp collisions | `:{post_id}` key suffix removes the failure class; seed also spaces posts |
| Go `time.Now().UnixNano()` clock resolution on Linux (~100ns granularity) | `:{post_id}` suffix absorbs ties; merge sorts secondarily by post ID |
| 1000-follower seed latency (1000 follow requests) | Batched B-side writes + parallel client; seed script uses concurrent follows (which the conditional-transaction + conflict-retry contract now makes safe) |
| Phase-5 resilience window (15s detection) makes the system visibly fail during demos | U10 defines read-retry-via-replica + queue behavior for the window; the resilience drill scripts it |

---

## Documentation / Operational Notes

- `design.md` is the interview deliverable and is written continuously (U1–U12) — it must never be backfilled at the end
- `docs/notes/` is the step-by-step learning journal maintained per-unit during execution — one file per phase (indexed via `docs/notes/README.md`), capturing concepts, decisions, gotchas, and interview talking points as work lands. Complements `design.md` (depth) with quick comprehension
- `plan.txt` stays the blueprint; it is amended once in U1 (key schema + read-path step 4) to remove the R3/R4 contradiction
- `README.md` (U12) carries the runbook: prerequisites, up, seed, demo, resilience drill
- No monitoring or operational tooling — a learning project; `/health`, `/cluster`, and coordinator fan-out logs are the observability surface

---

## Sources & References

- **Origin document:** [docs/brainstorms/distributed-social-feed-engine-requirements.md](docs/brainstorms/distributed-social-feed-engine-requirements.md)
- **Blueprint:** `plan.txt` (structure, API surface, env contract, build order — amended in U1)
- **External docs:** BadgerDB quickstart and cursor-pagination patterns — https://github.com/dgraph-io/badger/blob/main/docs/quickstart.md
