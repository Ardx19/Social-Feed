# Project Notes

A step-by-step learning journal for the Distributed Social Feed Engine — maintained throughout the build. Each file covers one build phase: concepts as they land, decisions with their rejected alternatives, the code structure, gotchas, and how to narrate it in an interview.

**Companion documents**
- `plan.txt` — the original execution blueprint
- `docs/brainstorms/distributed-social-feed-engine-requirements.md` — the WHAT (requirements)
- `docs/plans/2026-08-10-001-feat-distributed-feed-engine-plan.md` — the HOW (implementation plan)
- `design.md` — the polished formal design document (primary interview deliverable, written in parallel)

## Project in 10 lines

We're building a distributed system that solves the **fan-out problem**: when a user posts, how does that post reach every follower's feed efficiently — without wrecking your infrastructure at scale?

- **Fan-out** means copying a post into every follower's feed. Do it eagerly (on write) and a celebrity's post is copied to millions of feeds; do it lazily (on read) and every feed read is expensive.
- Real systems use a **hybrid**: fan out eagerly for normal users, but serve celebrities lazily on read via an index. We copy that.
- The backend is **sharded** (data split across 3 shard nodes) so no single machine holds everything — but that makes "find all followers" a cross-machine problem.
- A **coordinator** routes requests to the right shard using a **consistent hash ring** (hand-implemented, no library), so adding/removing machines moves only a fraction of keys.
- Storage is **BadgerDB** (embedded, pure Go), with msgpack + zstd encoding and carefully-designed keys so range scans are efficient.
- Each shard has a **replica** for async replication; a heartbeat detects failures and the coordinator fails reads over to replicas.
- Accepted gaps (documented, not fixed): no consensus protocol (Raft), no event queue (Kafka), async replication can lose recent data, no auth. These are the "what I'd do differently" interview answers.
- The real deliverable is the ability to **defend every tradeoff** — that's what `design.md` and these notes build.

## Phase map

| File | Phase | Concepts | Status |
|------|-------|----------|--------|
| `01-single-node.md` | 1 — Single node | Embedded storage, key design, compression, range scans, shard HTTP API | **In progress** (data layer done; HTTP server in U2) |
| `02-cluster.md` | 2 — Cluster | Consistent hashing, virtual nodes, request routing, Docker Compose | Not started |
| `03-social-graph.md` | 3 — Social graph | Sharding the graph, cross-shard writes, self-follow, atomic increments | Not started |
| `04-fanout.md` | 4 — Fan-out | Hybrid fan-out, celebrity threshold, author index, scatter-gather, feed merge | Not started |
| `05-resilience.md` | 5 — Resilience | Async replication, heartbeat, failure detection, failover, write queue | Not started |
| `06-polish.md` | 6 — Polish | Frontend, seed data, demo, design doc assembly | Not started |

## How to use these notes

- Read in order (`01` → `06`) for the full journey, or jump to a phase file for a topic.
- Each file ends with **Interview talking points** — practice narrating those.
- Update the Status column here as phases complete.
