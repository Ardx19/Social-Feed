# Phase 3 — Social Graph Works

> Learning target: how a social graph (who follows whom) is stored across shards, and the cost of cross-shard writes.

## What we built

The coordinator's graph surface: `POST /users` (creates a user with both self-follow edges in one transaction, returns 409 on a duplicate), `GET /users/{id}`, and `POST /follow` + `POST /unfollow`. Follow is a cross-shard write: the following edge lands on the follower's shard (a plain Set, naturally idempotent), and the count-bearing followers edge + FollowerCount increment lands on the followee's shard as one conditional transaction.

## Concepts learned

- **Denormalized two-direction graph:** a follow relationship is stored as TWO edges — `graph:following:{A}:{B}` (on A's shard) and `graph:followers:{B}:{A}` (on B's shard). Both "who follows X" and "who X follows" become O(scan). One direction would force an O(full scan) for the other.
- **Cross-shard writes are not atomic:** follow touches two shards. If the B-side fails after the A-side succeeds, you get a half-edge (A follows B but B doesn't know). Accepted + logged.
- **The count is a celebrity gate:** FollowerCount decides which fan-out path runs. It must always equal the number of real follower edges — which is why the follow transaction is conditional.
- **Read-modify-write under concurrency:** incrementing a count requires read → increment → write. Under concurrent follows this is a race; BadgerDB's optimistic concurrency aborts losers with `ErrConflict`, and the caller must retry.

## Key decisions + why

| Decision | Why | Rejected alternative |
|----------|-----|----------------------|
| Store both edge directions | Both "who follows X" and "who X follows" are O(scan) | One direction (other becomes full scan) |
| Self-follow at user creation (both edges) | Own posts flow through existing paths, no special cases | Merge own posts on read |
| Self-follow does NOT increment FollowerCount | Users born at 0; celebrities cross at exactly 1000 real followers | Count self |
| Unfollow-self rejected (400) | Otherwise a celebrity's own feed goes dark | Allow + document |
| Invariant: FollowerCount == real follower edges (minus self) | The count is the celebrity gate — it must mean exactly what it says | Let it drift |
| Conditional B-side transaction (lives in storage, not the handler) | `storage.AddFollower`/`RemoveFollower` do edge check + count change in ONE txn inside the codec-aware wrapper; the HTTP endpoint is a thin call. Duplicate follow = idempotent no-op; unfollow without edge = no-op | Generic increment endpoint; transaction in the api handler (leaks codec) |
| Bounded `badger.ErrConflict` retry with backoff | BadgerDB optimistic concurrency: concurrent commits to the same user record fail; `db.Update` doesn't retry — the losers must | No retry (concurrent follows lose increments) |
| Follow of a non-existent user → 404, nothing written | B-side surfaces ErrNotFound; A-side must not create a dangling following edge | Let it succeed |
| `POST /users` on existing ID → 409 | Re-creating resets FollowerCount and flips celebrity status; breaks seed idempotency | Overwrite |
| Shared ID validation | Reject `:`/control/whitespace everywhere (prefix-scan leakage); one helper, applied to every client ID | Per-handler checks |
| `GET /users/{user_id}` on the coordinator | Read user data without leaking key schema into handlers | Direct shard key reads |

## Structure map

- `internal/api/users.go` — `POST /users`, `GET /users/{id}` handlers
- `internal/api/graph.go` — `POST /follow`, `POST /unfollow` cross-shard orchestration
- `internal/api/validate.go` — the single ID-validation helper
- `internal/storage` — `CreateUser` (self-edges, `ErrExists`), `AddFollower`/`RemoveFollower` (conditional + retry)

## Gotchas & edge cases

- **Duplicate follow must be idempotent, not "increment twice":** the B-side transaction checks the edge exists before incrementing. A blind increment drifts the count from the real edge set, and the celebrity gate then decides fan-out on a meaningless number.
- **Hot-key contention:** every follow of a celebrity conflicts with every other (one user record), serialized by the retry loop with backoff. Fine at this scale; a separate count key is the future decoupling.
- **Space vs control chars:** IDs must reject whitespace (0x20) too, not just control chars — they end up in URL paths/query strings.
- **Half-edges:** a follow where the B-side fails leaves A-side-only state. Logged and accepted (R8).

## Interview talking points

- "How do you store a social graph across shards?" — two denormalized edges, one per direction, each on the appropriate side's shard, so both reads are prefix scans.
- "What's the catch?" — follow is a cross-shard write, not atomic. A half-edge is accepted and documented; the count is kept consistent with the B-side edge by making that side a single conditional transaction.
- "How do you keep FollowerCount correct under concurrency?" — one transaction that checks the edge, writes it, and increments, retried on `ErrConflict` with backoff. The count is the celebrity gate, so it can't be allowed to drift.
- "Why 409 on duplicate user creation?" — overwriting would reset FollowerCount and flip a celebrity back to normal, and it breaks seed idempotency.
