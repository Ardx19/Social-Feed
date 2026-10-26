# Phase 4 — Fan-out Works

> Learning target: the heart of the project — hybrid fan-out with a celebrity threshold, and the scatter-gather read path.

## What we built

`POST /posts` writes a post + author index to the author's shard in one transaction, checks FollowerCount, and either fans out a denormalized FeedEntry to every follower's shard (one batched write per shard, failures logged-and-skipped) or does nothing for a celebrity. `GET /feed/{user}` merges the fan-out entries with each celebrity's recent posts (fetched lazily via the author index), newest-first, deduped, with logical-cursor pagination.

## Concepts learned

- **Hybrid fan-out:** eager (on write) for normal users, lazy (on read) for celebrities — the Twitter-style tradeoff that bounds write amplification.
- **Write amplification:** a normal user's post is copied into every follower's feed. Denormalized content means a feed renders in one scan, at the cost of duplicated storage.
- **Celebrity threshold as a gate:** FollowerCount decides which path runs. This is why the count invariant matters so much.
- **Author index:** `posts_by_author:{author}:{ts}:{post}` makes "recent posts by celebrity" a single-shard range scan. `posts:{id}` can't — post IDs hash across shards.
- **Scatter-gather:** reading a feed fans out across potentially many shards in parallel (followee counts, celebrity scans), then merges.
- **Logical cursor:** a merged page spans multiple key prefixes on different shards, so the cursor can't be a single physical key — it's the `(ts, post_id)` position, and each source rebuilds its own seek key.
- **Arbitrary-but-deterministic ordering:** Go serves POSTs concurrently; two posts in the same nanosecond sort by post ID — stable, not creation order.

## Key decisions + why

| Decision | Why | Rejected alternative |
|----------|-----|----------------------|
| Hybrid fan-out (threshold 1000) | Eager for normal users, lazy for celebrities | Pure write / pure read fan-out |
| Author index `posts_by_author:{author}:{ts}:{post}` | Celebrity reads are a single-shard scan | Scanning all posts (unimplementable cross-shard) |
| Denormalized feed content | Feed renders in one scan, no N+1 fetches | Pointer-only FeedEntry + post fetch per entry |
| Coordinator stamps one ns timestamp per post | Single source of truth, no cross-shard clock skew | Shard-stamped timestamps |
| Same-ns ties arbitrary-but-deterministic | Go serves POSTs concurrently; secondary post-ID sort is stable, not creation order | Claim "creation order" |
| Merge dedupes by post ID | Threshold-crossing post appears via both paths; show it once | Let duplicates through |
| Post to down author shard → 502, never queued | Gate + fan-out pairs can't be materialized offline | Queue it (silent fan-out loss) |
| One BadgerDB transaction per shard batch | All-or-nothing per shard, matches log-and-skip failure model | Per-entry writes |
| Logical cursor `(ts, post_id)`, reconstructed per prefix | Merged pages span multiple prefixes/shards; one physical key can't seek into both | Physical-key cursor (drops/repeats celebrity posts across pages) |
| Post→author map for `GET /posts/{id}` | Random post IDs aren't routable by entity ID; map routes via the author | Scanning for the post |

## Structure map

- `internal/fanout/fanout.go` — `CreatePost`: materialize post+index, celebrity gate, batched fan-out
- `internal/fanout/reader.go` — `Merge`: sort, dedupe, truncate, logical cursor (pure)
- `internal/api/writer.go` — `POST /posts`, `GET /posts/{id}`, post→author map
- `internal/api/reader.go` — `GET /feed/{user}` orchestration, celebrity scatter-gather

## Gotchas & edge cases

- **Threshold crossing:** a post made before the author crosses the threshold appears via fan-out; the same post later fetched via the celebrity path would duplicate — dedupe by post_id in the merge kills it.
- **Celebrity own-posts need the self-edge in the followee set:** a celebrity's own feed is empty via fan-out (none happens); they see their own posts only because self is a followee AND a celebrity. Don't filter the self-edge out of the followee list.
- **Cursor must be logical, not a physical key:** once a celebrity is in the followee set, the merged page spans two prefixes; a single physical feed key can't seek into the celebrity's author index. Rebuild each source's seek key from `(ts, post_id)`.
- **Empty feed is `200 []`, not an error:** the missing-vs-error taxonomy. And a mistyped user ID should 404 (unknown user), not render a misleading empty feed.
- **Follower-ID extraction:** IDs are validated to contain no `:`, so parsing `graph:followers:{user}:{follower}` is a safe split on the last `:`.
