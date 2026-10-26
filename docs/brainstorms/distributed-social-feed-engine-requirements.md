---
date: 2026-08-09
topic: distributed-social-feed-engine
---

# Distributed Social Feed Engine — Requirements

## Summary

Build the distributed feed engine from `plan.txt` — Go coordinator with a self-implemented consistent hash ring routing to 3 shards (each with a replica), BadgerDB storage, hybrid fan-out with a celebrity threshold, Docker Compose, and a React demo frontend — refined by four decisions: an author index key for celebrity reads, auto self-follow so users see their own posts, durability gaps accepted and documented with production answers, and a design doc written interleaved with the build phases instead of end-loaded.

---

## Problem Frame

The fan-out problem: when a user posts, the post must reach every follower's feed. Eager fan-out is write-amplified and unbounded for celebrities; lazy fan-out makes reads expensive. Hybrid approaches are what production systems actually run. This project exists to make distributed systems concepts — consistent hashing, sharding, scatter-gather reads, fan-out strategies, CAP tradeoffs — concrete by building them, because the builder's primary goal is learning system design: the design doc and the ability to defend tradeoffs are the real deliverable, and working code is the evidence the reasoning was lived rather than memorized. The existing plan is thorough, but it is missing three decisions the code cannot be built without: how celebrity posts are found on read, whether users see their own posts, and which failure modes are acceptable.

---

## Actors

- A1. Builder (the user): builds the system phase by phase, reviews code, learns the concepts.
- A2. Reader (audience): reads the design doc; tradeoff reasoning and honest gap documentation are written for them.

---

## Key Flows

- F1. Post creation (write path)
  - **Trigger:** A user submits a post via the Post View or `POST /posts`
  - **Actors:** A1
  - **Steps:** Store post and author index entry on the author's shard → check author's FollowerCount → if below threshold, fan out a FeedEntry to each follower's shard in parallel, skipping followers on down shards and logging failures; if at or above threshold, do nothing more
  - **Outcome:** Post is durable; feeds of normal authors' followers are updated eagerly; celebrity posts are delivered lazily on read
  - **Covered by:** R1, R3, R5, R6, R7

- F2. Feed read (read path)
  - **Trigger:** A user opens their feed (Feed View or `GET /feed/{user_id}`)
  - **Actors:** A1
  - **Steps:** Range-scan precomputed feed entries on the user's shard → fetch the followee list → identify celebrities (FollowerCount >= threshold) → fetch their recent posts via the author index on their shards → merge all entries by timestamp descending and return the top N
  - **Outcome:** A merged chronological feed including celebrity posts; reads are a bounded scatter-gather of O(followees) shards
  - **Covered by:** R1, R3, R4, R6

- F3. Follow / unfollow
  - **Trigger:** A user follows or unfollows another user (`POST` / `DELETE /follow`)
  - **Actors:** A1
  - **Steps:** Write the following edge on the follower's shard; write the followers edge and update FollowerCount on the followee's shard
  - **Outcome:** Graph and count converge; a partial failure may leave a half-edge, accepted as a documented gap
  - **Covered by:** R1, R8

---

## Requirements

**[Execution blueprint]**
- R1. Build per `plan.txt`: Go coordinator with a self-implemented consistent hash ring routing to 3 shard nodes each with one replica, BadgerDB storage with zstd compression, HTTP+JSON transport, Docker Compose orchestration, React+Vite frontend. `plan.txt` is the source of truth for structure, API surface, data models, and build order.
- R2. Follow `plan.txt`'s six-phase build order (single node → cluster → social graph → fan-out → resilience → polish), with each phase independently verifiable before the next begins.

**[Key schema additions]**
- R3. Maintain an author index key — `posts_by_author:{author_id}:{timestamp_padded}` — written alongside every post, so recent posts by a given author are a single-shard range scan.
- R4. The celebrity read path (read-path steps 3–4 of `plan.txt`) fetches celebrity posts via the author index, never by scanning all posts.

**[Own posts in feed]**
- R5. User creation auto-creates a self-follow edge in **both** directions (following self and follower self), so own posts flow through existing paths with no special-casing.
- R6. A user sees their own posts in their feed: non-celebrities receive them via normal fan-out (self is in the author's follower set); celebrities receive them via the read path (self is in the user's followee set and qualifies as a celebrity).

**[Durability decisions]**
- R7. Fan-out writes to a down shard are logged and skipped; there is no backfill or retry queue. Documented in the design doc's known gaps with the production answer (event queue / Kafka).
- R8. Cross-shard follow writes (follower edge, followee edge, follower count) are not atomic; partial failure may produce a half-edge. Documented with the production answers (two-phase commit, outbox pattern).
- R9. All gaps in `plan.txt`'s known-gaps table remain gaps and are documented, not built: consensus protocol, synchronous replication, event queue, feed cache, gRPC, dynamic celebrity threshold, auth, multi-coordinator.

**[Design doc]**
- R10. The design doc is written interleaved with the build — each section as its concept is learned (key design after Phase 1, hashing and routing after Phase 2, fan-out after Phase 4, CAP and replication after Phase 5) — with final assembly, including "what breaks at scale" and "what you'd do differently", at the end.

**[Demo]**
- R11. Frontend has exactly the three views from `plan.txt` (Feed, Post, Follow): plain React with `useState` + fetch, no auth, no sessions, no state management library.
- R12. Phase 6 seed data includes at least one celebrity (FollowerCount >= threshold) so both fan-out modes are observable in the demo.

---

## Acceptance Examples

- AE1. **Covers R5, R6.** Given a freshly created user A with no followees besides self, when A posts, A's feed contains the post.
- AE2. **Covers R4, R6.** Given celebrity C (FollowerCount at/above threshold), when C posts, no fan-out writes occur, and C's feed still contains the post via the read path.
- AE3. **Covers R3, R4.** Given follower F following celebrity C, when C posts and F reads their feed, C's post appears without scanning any of C's non-author posts.
- AE4. **Covers R7.** Given a shard is down, when a non-celebrity posts, the post is stored, feed entries to followers on the down shard are skipped and logged, and the system keeps serving; after recovery, the missing entries are not backfilled.

---

## Success Criteria

- The builder can articulate, unprompted: why consistent hashing, why BadgerDB, why hybrid fan-out, where the threshold sits and why, the system's CAP position, and what breaks at scale.
- The design doc is complete and honest — every accepted gap named with its production answer.
- All six phases complete; the end-to-end demo shows both fan-out modes; the feed merge is correct for celebrity and non-celebrity authors.
- The decisions in this document are reflected in the built system without drift.

---

## Scope Boundaries

- Guaranteed fan-out delivery (event queue / Kafka): documented only, not built.
- Atomic follow writes (two-phase commit / outbox): documented only, not built.
- Everything in `plan.txt`'s known-gaps table (Raft, synchronous replication, Redis feed cache, gRPC, dynamic threshold, auth, multi-coordinator): not built.
- Feed backfill / repair on shard recovery: not built.
- Frontend beyond the three views: not built.

---

## Key Decisions

- **Author index mirror write:** celebrity reads need a single-shard scan; the write-amplification cost is the standard tradeoff in real systems, and the key shape mirrors how production systems index posts by author.
- **Self-follow with both edges on user creation:** own posts flow through existing paths with zero special cases; both edges are required because non-celebrities need the follower edge (fan-out delivers own feed) and celebrities need the following edge (read path picks self up).
- **Accept partial fan-out and cross-shard follow gaps:** learning-project scope; each gap has a known production answer (Kafka for fan-out, 2PC/outbox for multi-shard writes).
- **Interleaved design doc:** the reasoning is fresh when written and compounds learning across phases; end-loaded writing (as planned) risks losing the "why" behind early decisions.
- **Learning is the primary goal:** tradeoff reasoning and the design doc outrank polish, coverage completeness, and feature breadth.

---

## Dependencies / Assumptions

- Builder has solid Go experience including concurrency (stated in dialogue).
- Docker is available for the Compose cluster.
- No auth: the coordinator API is open and demo-accessible.
- `plan.txt` remains the execution blueprint; this document refines it — where they conflict, this document governs.

---

## Outstanding Questions

### Deferred to Planning

- [Affects R3][Technical] Exact timestamp padding width and epoch format for the sortable keys in the author index and feed keys.
- [Affects R5][Technical] Whether the self-follow edge can be removed via the normal unfollow API (and what happens if it is).
