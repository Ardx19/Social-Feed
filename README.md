# Distributed Social Feed Engine

A distributed system that solves the **fan-out problem** — getting a post to every follower's feed efficiently across a sharded backend. Built to make distributed-systems concepts concrete (consistent hashing, sharding, scatter-gather, hybrid fan-out, CAP tradeoffs, async replication). `design.md` is the design document defending every tradeoff.

## Architecture

- **Coordinator** (Go): consistent hash ring routing, hybrid fan-out, failover, write queue.
- **3 shards + 3 replicas** (Go, BadgerDB): range scans, conditional count-bearing transactions, async replication.
- **React + Vite frontend**: three views (Feed, Post, Follow).

```
coordinator (8080)
  └── shard1 (8081) ←→ replica1 (8084)
  └── shard2 (8082) ←→ replica2 (8085)
  └── shard3 (8083) ←→ replica3 (8086)
```

## Prerequisites

- Go 1.25+
- Node 20+ (frontend)
- Docker + Docker Compose (or `scripts/cluster.sh` if `docker-compose` v1 breaks against Docker 29)

## Full instructions

`docs/RUNBOOK.md` is the complete runbook: from-scratch setup, how the project runs internally (startup, routing, fan-out, replication, failover), the frontend explained view-by-view with a click-through demo, operations, and troubleshooting.

## Run the cluster

```bash
# build the backend image + bring up 7 containers
docker-compose build
docker-compose up -d
# ...or, if docker-compose v1 fails with a ContainerConfig KeyError:
bash scripts/cluster.sh
```

Verify: `curl localhost:8080/cluster` lists 6 data nodes, all reachable.

## Seed + verify

```bash
# seed a small graph + a 1001-follower celebrity (threshold 1000 + 1 margin,
# so a single lost follow increment can't silently flip the celebrity path)
go run ./scripts/seed -coordinator http://localhost:8080

# assert the demo claims (5 checks, incl. celeb's exact follower count)
bash scripts/demo/verify.sh http://localhost:8080

# optional narrated demo: both fan-out modes + the failover drill with
# commentary and live output
bash scripts/demo/run.sh http://localhost:8080
```

## Frontend

```bash
cd frontend
npm install
npm run dev   # http://localhost:5173 (proxies /api -> localhost:8080)
```

Try: Feed View `alice` (own post + follows), `f000` (sees the celebrity post lazily), `celeb` (own posts via read path). Post View to create posts. Follow View to follow/unfollow.

## Quick API tour

```bash
curl -X POST localhost:8080/users -d '{"id":"alice","name":"Alice"}'
curl -X POST localhost:8080/follow -d '{"follower":"bob","followee":"alice"}'
curl -X POST localhost:8080/posts -d '{"author_id":"alice","content":"hi"}'
curl "localhost:8080/feed/bob"        # fan-out entries + celebrity lazy posts
curl localhost:8080/users/alice       # {id, name, follower_count} — the celebrity gate
curl "localhost:8080/users/alice/following"
```

## Resilience demo

Container names differ by launch method (compose v2 uses dashes, `cluster.sh`
underscores), and no endpoint exposes the key→shard mapping — so discover the
shard and probe for affected users instead of hardcoding:

```bash
shard=$(docker ps --format '{{.Names}}' | grep -E 'shard[0-9]' | grep -vi replica | head -1)
docker stop "$shard"
# ~15s for the heartbeat to mark it down (5s interval, 3 misses)
curl localhost:8080/cluster
# reads still return data — served from the replica (async replication + failover)
# posts to the down shard are never queued — probe which users live there:
for u in alice bob carol celeb f000; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST localhost:8080/posts \
    -d "{\"author_id\":\"$u\",\"content\":\"probe\"}")
  [ "$code" = "502" ] && echo "$u: 502 (author shard is the down one)"
done
docker start "$shard"
# ~10s; /cluster shows it back up; queued writes drain; posts resume
```

## Visual demo (no cluster needed)

`demo/simulation.html` is a self-contained, animated simulation of the whole system: the hash ring, the coordinator, 3 shards + 3 replicas, packets on the wire, a live storage inspector, and narrated scenarios (routing, fan-out on write, celebrity lazy reads, threshold crossing, async replication, failover drill, free play). Open the file in any browser; no build step, Docker or Go required. Use Space to play/pause and the right arrow to step.

It re-implements the routing, fan-out, replication and failover logic in JavaScript for presentation. The Go code and `scripts/demo/run.sh` remain the real system.

## Learning notes

`docs/notes/` is a per-phase journal: concepts, decisions + rejected alternatives, structure maps, and gotchas. Read `01` → `06` in order.

## Documents

- `design.md` — the design document (decisions, tradeoffs, what breaks at scale)
- `docs/notes/` — step-by-step learning journal
- `docs/plans/2026-08-10-001-feat-distributed-feed-engine-plan.md` — the implementation plan
- `docs/brainstorms/distributed-social-feed-engine-requirements.md` — the requirements
