# Distributed Social Feed Engine

A distributed system that solves the **fan-out problem** — getting a post to every follower's feed efficiently across a sharded backend. Built to make distributed-systems concepts concrete (consistent hashing, sharding, scatter-gather, hybrid fan-out, CAP tradeoffs, async replication). The real deliverable is `design.md` — the interview-ready document defending every tradeoff.

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
# seed a small graph + a 1000-follower celebrity (both fan-out modes)
go run ./scripts/seed -coordinator http://localhost:8080

# assert the demo claims
bash scripts/demo/verify.sh http://localhost:8080
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
curl "localhost:8080/users/alice/following"
```

## Resilience demo (the interview narrative)

1. `docker stop social_feed_shard2_1` — wait ~16s for the heartbeat to mark it down.
2. `curl localhost:8080/cluster` — shard2 `reachable: false`.
3. Reads of a shard2 user still return data — served from replica2 (async replication + failover).
4. `curl -X POST localhost:8080/posts -d '{"author_id":"<shard2-user>",...}'` → `502` (post creation is never queued).
5. `docker start social_feed_shard2_1` — wait ~10s; `/cluster` shows it back up; writes resume.

## Learning notes

`docs/notes/` is a per-phase journal: concepts, decisions + rejected alternatives, structure maps, gotchas, and interview talking points. Read `01` → `06` in order.

## Documents

- `design.md` — the interview deliverable (decisions, tradeoffs, what breaks at scale)
- `docs/notes/` — step-by-step learning journal
- `docs/plans/2026-08-10-001-feat-distributed-feed-engine-plan.md` — the implementation plan
- `docs/brainstorms/distributed-social-feed-engine-requirements.md` — the requirements
