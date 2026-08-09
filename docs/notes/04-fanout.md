# Phase 4 — Fan-out Works

> Learning target: the heart of the project — hybrid fan-out with a celebrity threshold, and the scatter-gather read path.

## What we built

*(filled in after the phase)*

## Concepts learned

*(fan-out on write vs read vs hybrid, write amplification, celebrity threshold, author index, scatter-gather, dedupe, cursor pagination, coordinator-stamped timestamps)*

## Key decisions + why

| Decision | Why | Rejected alternative |
|----------|-----|----------------------|
| Hybrid fan-out (threshold 1000) | Eager for normal users, lazy for celebrities — the Twitter-style tradeoff | Pure write fan-out / pure read fan-out |
| Author index `posts_by_author:{author}:{ts}:{post}` | Celebrity reads are a single-shard scan | Scanning all posts (unimplementable cross-shard) |
| Denormalized feed content | Feed renders in one scan, no N+1 fetches | Pointer-only FeedEntry + post fetch per entry |
| Coordinator stamps one ns timestamp per post | Single source, no cross-shard clock skew, mergeable | Shard-stamped timestamps |
| Same-ns ties are arbitrary-but-deterministic | Go serves POSTs concurrently; secondary post-ID sort is stable, not creation order — say it precisely | Claim "creation order" |
| Merge dedupes by post ID | Threshold-crossing post appears via both paths; show it once | Let duplicates through |
| **Pagination cursor is LOGICAL `(ts19, post_id)`, not a physical key** | Merged pages span `feed:{user}:*` and celebrity `posts_by_author:*` on different shards — one physical key can't seek into both. Each scan reconstructs its own prefix's seek key from the tuple | Physical-key cursor (drops/repeats celebrity posts across pages once one is in the mix) |
| Post to down author shard → 502, never queued | Gate + fan-out pairs can't be materialized offline | Queue it (silent fan-out loss) |
| One BadgerDB transaction per shard batch | All-or-nothing per shard, matches log-and-skip failure model | Per-entry writes |

## Structure map

- `internal/fanout/writer.go` — post materialization + batched fan-out
- `internal/fanout/reader.go` — merge + dedupe + cursor pagination
- `internal/api/coordinator.go` — post/feed handlers; in-memory post→author map

## Gotchas & edge cases

*(threshold crossing both directions, empty feed = 200 [], deterministic tie order, partial fan-out loss on a down shard)*
*(unfollow residue: non-celebrity posts already fanned out stay in the follower's feed forever (no cleanup job) while celebrity posts vanish instantly — the write-path/read-path asymmetry; named gap with production answer: cleanup job / tombstone-on-read)*

## Interview talking points
