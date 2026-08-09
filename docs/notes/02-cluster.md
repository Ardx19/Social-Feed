# Phase 2 — Cluster Works

> Learning target: consistent hashing, a coordinator that routes by entity ID, and the whole thing running under Docker Compose.

## What we built

*(filled in after the phase)*

## Concepts learned

*(consistent hashing, why it beats modulo, virtual nodes, routing by entity ID vs full key, scatter vs the ring's single lookup, static ring membership)*

## Key decisions + why

| Decision | Why | Rejected alternative |
|----------|-----|----------------------|
| Consistent hash ring, hand-implemented | Core concept; no library | A library |
| 150 virtual nodes per physical node | Even distribution across 3 shards | 1 node position each (unbalanced) |
| Route by entity ID only | Range scans need all of an entity's keys on one shard | Hashing the full key (scatters scans) |
| Static ring membership | Re-hashing on failure strands keys; fail over instead | `RemoveNode` on failure |
| Single binary `./app` with subcommands | One build, 7 compose services dispatch on subcommand | Two binaries |

## Structure map

*(one line per package/file)*

- `internal/hash` — the consistent hash ring
- `cmd/coordinator` — coordinator entrypoint
- `internal/api/coordinator.go` — public HTTP surface (stubs here)
- `internal/api/client.go` — shard HTTP client; classifies missing / shard-error / down
- `Dockerfile`, `docker-compose.yml` — the 7-service cluster

## Gotchas & edge cases

## Interview talking points
