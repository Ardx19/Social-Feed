# Phase 2 — Cluster Works

> Learning target: consistent hashing, a coordinator that routes by entity ID, and the whole thing running under Docker Compose.

## What we built

The hash ring (U3) and the coordinator (U4): a single `./app coordinator` that owns the ring, routes every entity ID to its shard via `ShardFor`, wraps shard calls in a client that classifies missing / shard-error / down, and exposes `/health` + `/cluster`. The public API surface (users/follow/posts/feed) is wired as 501 stubs to be filled by U6–U8.

## Concepts learned

- **Consistent hashing:** keys hash onto a circle; each node owns the clockwise arc to the next. Adding/removing a node moves only ~1/N of keys (vs. ~all for naive modulo).
- **Virtual nodes:** replicating each physical node 150× across the ring evens distribution — with one position each, a node can own most of the arc by luck.
- **Hash quality matters:** FNV-1a's weak avalanche made sequential keys cluster (one node got 67%); MD5's strong diffusion fixed it (~33/33/33).
- **Routing by entity ID, not the full key:** a range scan (`feed:{user}:*`) needs every key of an entity on one shard, so the ring hashes the entity ID, never a full storage key.
- **Static ring membership:** removing a failed node re-hashes and strands keys; failure means failover to a replica, not ring mutation.
- **Error taxonomy:** the coordinator classifies shard responses into missing (200 + null), shard-error (5xx), down (connection/timeout). Missing is NOT an error — an empty feed must render as empty, not fail.

## Key decisions + why

| Decision | Why | Rejected alternative |
|----------|-----|----------------------|
| Consistent hash ring, hand-implemented | Core concept; no library | A library |
| 150 virtual nodes per physical node | Even distribution across 3 shards | 1 position each (unbalanced) |
| MD5 over FNV for hashing | Strong avalanche; sequential keys don't cluster | FNV-1a (skewed 67/17/15) |
| Route by entity ID only | Range scans need all of an entity's keys on one shard | Hashing the full key |
| Static ring membership | Re-hashing on failure strands keys; fail over instead | `RemoveNode` on failure |
| Single binary `./app` with subcommands | One build, 7 compose services dispatch on subcommand | Two binaries |
| Error taxonomy (missing/shard-error/down) | Coordinator must distinguish "empty" from "failed" and "down" | Confusing all three |
| Public API stubbed at U4 | Surface exists and is testable before handlers land | Wiring handlers ad hoc |

## Structure map

- `internal/hash` — the consistent hash ring (virtual nodes, MD5, binary-search clockwise)
- `internal/api/coordinator.go` — ring + topology + `ShardFor` + `/health` + `/cluster` + public-API stubs
- `internal/api/client.go` — shard HTTP client; classifies missing/shard-error/down; base64 values on the wire
- `cmd/app` — `./app coordinator` and `./app shard`
- `Dockerfile` — single multi-stage build producing `/app`
- `docker-compose.yml` — 7 services, named volumes, internal-only shard ports
- `scripts/cluster.sh` — raw-docker cluster bring-up (workaround for docker-compose v1 vs Docker 29)

## Gotchas & edge cases

- **FNV clustering:** the distribution test caught this — sequential keys are correlated under FNV-1a. The lesson: verify distribution with a test, don't assume.
- **Empty ring:** `GetNode` on an empty ring must error clearly, not panic.
- **Query escaping:** keys contain `:` and `{}`; naive percent-replacement breaks on `&`/`#` — use `url.QueryEscape`, not a hand-rolled escaper.
- **Topology parse form:** `SHARDS=nodeID:port` is the two-segment form where host == nodeID (Compose service name is the host). A three-segment `nodeID:host:port` form allows an explicit host. Getting this wrong silently drops all nodes.
- **Compose v1 vs Docker 29:** `docker-compose up` can fail with a `ContainerConfig` KeyError on newer Docker daemons. `scripts/cluster.sh` uses raw `docker run` with the right `--network-alias` (the alias is what lets the coordinator resolve `shard1`, not the container name) as a drop-in.

## Interview talking points

- "Why consistent hashing over modulo?" — modulo moves ~all keys on reshard; a ring moves ~1/N. Explain the circle and virtual nodes.
- "Why 150 virtual nodes?" — even distribution. With few positions, one node can own most of the arc.
- "What happens when a node dies?" — we do NOT remove it from the ring (that strands keys); we fail reads over to its replica. Consistent hashing minimizes reshard cost, it doesn't eliminate it.
- "Why route by entity ID?" — a range scan must stay on one shard; hashing the full key would scatter it.
- "How do you tell empty from failed?" — the error taxonomy: missing vs shard-error vs down. An empty feed is `200 []`, never a 500.
