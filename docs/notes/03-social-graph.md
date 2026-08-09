# Phase 3 — Social Graph Works

> Learning target: how a social graph (who follows whom) is stored across shards, and the cost of cross-shard writes.

## What we built

*(filled in after the phase)*

## Concepts learned

*(denormalized two-direction graph, why you store both follower + following edges, cross-shard writes, non-atomicity / half-edges, read-modify-write vs atomic increment)*

## Key decisions + why

| Decision | Why | Rejected alternative |
|----------|-----|----------------------|
| Store both edge directions | Both "who follows X" and "who X follows" are O(scan) | One direction (other becomes full scan) |
| Self-follow at user creation (both edges) | Own posts flow through existing paths, no special cases | Merge own posts on read |
| Self-follow does NOT increment FollowerCount | Users born at 0; celebrities cross at exactly 1000 real followers | Count self |
| Unfollow-self rejected (400) | Otherwise a celebrity's own feed goes dark | Allow + document |
| **Invariant: FollowerCount == real follower edges (minus self)** | The count is the celebrity gate — it must mean exactly what it says | Let it drift |
| Conditional B-side transaction (lives in storage, not the handler) | `storage.AddFollower`/`RemoveFollower` do edge check + count change in ONE txn inside the codec-aware wrapper; the HTTP endpoint is a thin call. Duplicate follow = idempotent no-op; unfollow without edge = no-op | Generic increment endpoint; transaction in the api handler (leaks codec) |
| Follow of a non-existent user → 404, nothing written | B-side surfaces ErrKeyNotFound; A-side must not create a dangling following edge | Let it succeed |
| Bounded `badger.ErrConflict` retry | BadgerDB optimistic concurrency: concurrent commits to the same user record fail; `db.Update` doesn't retry — the losers must | No retry (concurrent follows lose increments) |
| `POST /users` on existing ID → 409 | Re-creating resets FollowerCount and flips celebrity status; breaks seed idempotency | Overwrite |
| `GET /users/{user_id}` on the coordinator | Read user data without leaking key schema into handlers | Direct shard key reads |

## Structure map

- `internal/api/users.go` — user + follow/unfollow handlers
- `internal/fanout/graph.go` — edge-write orchestration across shards

## Gotchas & edge cases

*(half-edges, duplicate follow = idempotent no-op, unfollow-without-edge = no-op, ID validation / prefix-scan leakage, concurrent-follow convergence via conflict retry, hot-key contention on celebrity user records)*

## Interview talking points
