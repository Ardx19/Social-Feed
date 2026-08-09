# Phase 2 — Cluster Works

> Learning target: consistent hashing, a coordinator that routes by entity ID, and the whole thing running under Docker Compose.

## What we built

*(coordinator + ring + compose — filled in after U4/U5)*

## Concepts learned

- **Consistent hashing:** keys hash onto a circle; each node owns the clockwise arc to the next. Adding/removing a node moves only ~1/N of keys (vs. ~all for naive modulo).
- **Virtual nodes:** replicating each physical node 150× across the ring evens distribution — with one position each, a node can own most of the arc by luck.
- **Hash quality matters:** FNV-1a's weak avalanche made sequential keys cluster (one node got 67%); MD5's strong diffusion fixed it (~33/33/33).
- **Routing by entity ID, not the full key:** a range scan (`feed:{user}:*`) needs every key of an entity on one shard, so the ring hashes the entity ID, never a full storage key.
- **Static ring membership:** removing a failed node re-hashes and strands keys; failure means failover to a replica, not ring mutation.

## Key decisions + why

| Decision | Why | Rejected alternative |
|----------|-----|----------------------|
| Consistent hash ring, hand-implemented | Core concept; no library | A library |
| 150 virtual nodes per physical node | Even distribution across 3 shards | 1 position each (unbalanced) |
| MD5 over FNV for hashing | Strong avalanche; sequential keys don't cluster | FNV-1a (skewed 67/17/15) |
| Route by entity ID only | Range scans need all of an entity's keys on one shard | Hashing the full key |
| Static ring membership | Re-hashing on failure strands keys; fail over instead | `RemoveNode` on failure |
| Single binary `./app` with subcommands | One build, 7 compose services dispatch on subcommand | Two binaries |

## Structure map

- `internal/hash` — the consistent hash ring (virtual nodes, MD5, binary-search clockwise)
- `cmd/coordinator` — *(U4)*
- `internal/api/coordinator.go` — *(U4)*
- `internal/api/client.go` — *(U4)*
- `Dockerfile`, `docker-compose.yml` — *(U5)*

## Gotchas & edge cases

- **FNV clustering:** the distribution test caught this — sequential keys are correlated under FNV-1a. The lesson: verify distribution with a test, don't assume.
- **Empty ring:** `GetNode` on an empty ring must error clearly, not panic (indexing into an empty slice).

## Interview talking points

- "Why consistent hashing over modulo?" — modulo moves ~all keys on reshard; a ring moves ~1/N. Explain the circle and virtual nodes.
- "Why 150 virtual nodes?" — even distribution. With few positions, one node can own most of the arc.
- "What happens when a node dies?" — we do NOT remove it from the ring (that strands keys); we fail reads over to its replica. Consistent hashing minimizes reshard cost, it doesn't eliminate it.
- "Why route by entity ID?" — a range scan must stay on one shard; hashing the full key would scatter it.
