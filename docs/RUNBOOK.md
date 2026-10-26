# Runbook — Distributed Social Feed Engine

Everything you need to take the project from a fresh clone to a running demo,
understand *how* it runs, and drive the frontend. If something here disagrees
with the code, `design.md` is the source of truth for the *why*.

---

## 1. What is running, in one picture

A single Go binary (`./app`) is dispatched into **two roles** by subcommand:

| Role | Subcommand | Runs as | Count |
|---|---|---|---|
| Coordinator | `./app coordinator` | API gateway + router | 1 |
| Shard (primary) | `./app shard` | BadgerDB data node, replicates to a replica | 3 |
| Shard (replica) | `./app shard` (ROLE=replica) | BadgerDB copy, serves reads during failover | 3 |

```
you (curl / frontend)
   │  localhost:8080
   ▼
coordinator   (hash ring, hybrid fan-out, failover, write queue)
   ├── shard1  (8081) ←async replication→ replica1 (8084)
   ├── shard2  (8082) ←async replication→ replica2 (8085)
   └── shard3  (8083) ←async replication→ replica3 (8086)
```

- Only the **coordinator** is published to the host (port 8080). Shards and
  replicas talk only on the Docker network.
- Everything is simulated on one machine — the "distributed" part is that the
  coordinator has **no shared state** with the shards and must route each
  request over HTTP.

---

## 2. Prerequisites

| Tool | Version | Needed for |
|---|---|---|
| Go | 1.25+ | seeding + running tests; building the image |
| Node.js | 20+ | frontend dev server |
| Docker | any recent | the cluster (7 containers) |
| Docker Compose | v2 preferred | `docker-compose up` convenience |

> **Environment gotcha (this machine):** `docker-compose` v1.29 crashes against
> Docker 29 with a `ContainerConfig` KeyError. If you see that, use
> `bash scripts/cluster.sh` instead — it does the exact same thing with raw
> `docker` commands. `docker-compose.yml` remains the canonical definition.

---

## 3. Running from scratch (first time, in order)

```bash
# 0. One-time: build the backend image (contains ./app)
docker-compose build            # or: docker build -t social-feed .

# 1. Start the 7-node cluster
docker-compose up -d            # ...or, if compose v1 is broken:
bash scripts/cluster.sh

# 2. Wait a few seconds, then confirm all 6 data nodes are reachable
curl localhost:8080/cluster

# 3. Seed the demo graph (small users + a 1001-follower celebrity)
go run ./scripts/seed -coordinator http://localhost:8080

# 4. Prove the fan-out claims end-to-end (5 checks, expects VERIFY PASSED).
#    Asserts celeb's exact follower count (1001) — feeds populate either way,
#    so the count is the only guard against a silently flipped celebrity path.
bash scripts/demo/verify.sh http://localhost:8080

# 4b. Optional narrated walkthrough (both fan-out modes + the failover drill,
#     printing what each step proves): 
bash scripts/demo/run.sh http://localhost:8080

# 5. Frontend
cd frontend
npm install                     # one-time
npm run dev                     # http://localhost:5173
```

You are done. The demo surface is ready at http://localhost:5173.

### What the seed creates

`scripts/seed/main.go` (order matters — **no backfill** in this system, so
follows must exist before posts for fan-out to work):

1. `alice`, `bob`, `carol`, `celeb`
2. `alice→bob`, `bob→alice`, `carol→alice`
3. `f000`…`f1000` (1001 users, created concurrently with 50 workers) who all
   follow `celeb` — one above the celebrity threshold (1000), so a single lost
   follow increment can't silently flip `celeb` out of the celebrity path
4. One post each from `alice`, `bob`, `celeb`

That gives you both fan-out modes live: `alice`'s post is **pushed** to
followers, `celeb`'s post is **lazily read** from the author index.

> Reseed safely: user creation is idempotent (409 = already exists), so
> re-running the seed after a cluster restart is fine — but posts are
> re-created, so feeds get duplicates. For a clean rerun, reset (Section 6).

---

## 4. How the project runs — internals

### 4.1 Startup

**Shard node** (`cmd/app/main.go:64` `runShard`, env: `NODE_ID`, `PORT`,
`ROLE`, `REPLICA`, `DATA_DIR`):

1. Opens BadgerDB at `DATA_DIR` with a msgpack+zstd codec.
2. **Only if `ROLE=primary` and `REPLICA` is set:** constructs a replicator
   (bounded channel, size 4096) and registers a commit hook — every committed
   write is replayed to the replica as absolute values.
3. Serves the shard HTTP API (`internal/api/shard`): local read/write ops the
   coordinator proxies to it.

**Coordinator** (`cmd/app/main.go:34` `runCoordinator`, env: `PORT`, `SHARDS`,
`REPLICAS`, `CELEBRITY_THRESHOLD`):

1. Parses the static topology: `SHARDS=shard1:8081,shard2:8082,shard3:8083`,
   `REPLICAS=shard1:replica1:8084,…`.
2. Builds a **consistent hash ring** (MD5, 150 virtual nodes per shard — see
   the FNV-rejection story in `docs/notes/03-sharding.md`).
3. Starts per-node **heartbeats** (5s interval, 3 consecutive misses → down, ≈15s detection) and a write
   queue for offline-materializable writes.
4. Serves the public API: `/users`, `/follow`, `/unfollow`, `/posts`,
   `/feed/{user}`, `/users/{id}/following`, `/cluster`.

### 4.2 Request flows

**Routing** — every entity ID (user, post) is hashed to a shard. The
coordinator **never stores application data**; each request fans out over
HTTP to 1 or 3 shards.

**Follow** (`POST /follow`) — idempotent conditional transaction *on one
shard*: checks the edge doesn't exist, increments the followee's follower
count, writes both — in a **single Badger transaction** with
`badger.ErrConflict` retry (cap 1000, 50µs→2ms backoff). Self-follows create
**both** edges (the system invariant that makes "my own posts" work).

**Post** (`POST /posts`) — the fan-out decision:

- `follower_count < threshold` → **push**: write post to author's shard +
  write a copy into every follower's feed index (`fanout` package, concurrent
  writes to up to 3 shards).
- `follower_count >= threshold` (celebrity) → **no fan-out**: write once; the
  author index (`posts_by_author:{author}:{ts19}:{post_id}`) delivers to
  followers **lazily on read**.
- If the author's shard is down → **502 immediately** (post creation is never
  queued; the queue is only for follows/unfollows/deletes).

**Feed** (`GET /feed/{user}`) — scatter-gather:

1. Reads fan-out entries from the user's feed index (on their shard).
2. Reads the users they follow with `follower_count >= threshold` (author
   index read path).
3. Merge, sort by `(timestamp, post_id)` descending, paginate via a **logical
   cursor** (`before=<cursor>`) — `limit` defaults to 20.

**Error taxonomy** (surfaced to the frontend): shard-level "missing" reads are
`200` + `null`; the public API maps unknown users/posts to `404`; shard error =
`5xx`; down shard = connection/timeout.

**Replication** — one local transaction → one replication op → one transaction
on the replica (absolute values, ordered, single-drain channel, drop-newest on
overflow). Replicas never construct their own replicator (explicit startup
branch, `cmd/app/main.go:84`).

**Failover** — heartbeat marks a shard down after ~15s. The coordinator then
routes reads to the replica; writes to that shard either fail (posts, 502) or
queue (follow/unfollow edges) and flush when the shard returns.

### 4.3 Configuration knobs

| Env var | Where | Default | Meaning |
|---|---|---|---|
| `SHARDS` | coordinator | (required) | `host:port` topology |
| `REPLICAS` | coordinator | (required) | `shard:replica:port` topology |
| `CELEBRITY_THRESHOLD` | coordinator | 1000 | fan-out vs. lazy read cutoff |
| `NODE_ID` | shard | `shard` | node identity |
| `PORT` | both | `8080`/`8081` | HTTP port |
| `ROLE` | shard | `primary` | `primary` or `replica` |
| `REPLICA` | shard | empty | replica address for primaries |
| `DATA_DIR` | shard | `/data/<id>` | BadgerDB location |

To lower the demo threshold, edit `docker-compose.yml` (or `cluster.sh`) and
recreate the coordinator only.

---

## 5. The frontend, explained

### 5.1 Stack and wiring

- **React 18 + Vite 5** (`frontend/package.json`). No router, no state
  library, no CSS framework — one `App.jsx` with a tab switch.
- **API base:** `src/App.jsx` exports `apiBase = '/api'`. Vite's dev server
  proxies `/api/*` → `http://localhost:8080` and **rewrites the `/api` prefix
  away** (`frontend/vite.config.js:10`) because the backend routes live at the
  root (`/users`, `/feed/...`).
- **`api(path, options)`** (`src/App.jsx:14`) — small fetch wrapper that
  parses JSON, throws `{status, message}`, and surfaces the HTTP status.
- **`errorText(err)`** (`src/App.jsx:32`) — maps the backend's error taxonomy
  to human copy: `400` bad request, `404` not found, `502` "Feed unavailable"
  (shard down), `503` busy, anything else "Connection to the coordinator
  failed". This is the failover story made visible.

### 5.2 The three views

| View | File | Backend calls | What it demonstrates |
|---|---|---|---|
| **Feed** | `src/views/FeedView.jsx` | `GET /feed/{user}?limit=20&before=<cursor>` | Merged feed with load-more pagination (cursor chain) |
| **Post** | `src/views/PostView.jsx` | `POST /posts` | Create a post; watch it push to followers (or stay lazy for celebrities) |
| **Follow** | `src/views/FollowView.jsx` | `POST /follow`, `POST /unfollow`, `GET /users/{id}/following` | Conditional follows, unfollows, and the follow list |

**Feed view** — type a user ID, Enter. Renders `entries` with author, content,
and timestamp (ns → local time via `new Date(e.timestamp / 1e6)`). "Load more"
appends the next cursor page; empty responses show "No more posts". A failed
request renders the error mapping above (e.g. try a shard-down user to see
"Feed unavailable, try again").

**Post view** — author ID + content. On success shows the created `post_id`
and clears the content box. Validation is client-side (both fields required).

**Follow view** — a *viewer* (who "I" am), a follower/followee pair, and
Follow/Unfollow buttons. After each action it refreshes the following list
from the backend so the UI always reflects real state. The self-edge is a
system invariant and is not rendered as a user-managed follow (see `design.md`).

### 5.3 Suggested click-through demo

1. **Feed** `alice` → own post + bob's post (self-fan-out + fan-out).
2. **Feed** `f000` → the celebrity post appears **lazily** (author index read
   path — the celebrity fan-out story).
3. **Post** as `alice` ("hi from the UI") → **Feed** `bob` again → the new
   post is there (pushed fan-out).
4. **Feed** `celeb` → own posts via read path (no fan-out happened).
5. **Follow** viewer `carol`: follow `bob`, confirm list updates; unfollow,
   confirm it disappears.
6. **Failover live** — don't hardcode a shard or guess who lives there; discover
   both. Container names differ by launch method (compose v2 uses dashes,
   `cluster.sh` underscores):
   ```bash
   shard=$(docker ps --format '{{.Names}}' | grep -E 'shard[0-9]' | grep -vi replica | head -1)
   docker stop "$shard"
   sleep 16          # heartbeat: 5s interval, 3 misses ≈ 15s
   # Reads for everyone still work (replica failover) — Feed any user.
   # Find who lived on the down shard by probing: posts are never queued,
   # so the 502s are exactly the affected users:
   for u in alice bob carol celeb f000; do
     code=$(curl -s -o /dev/null -w '%{http_code}' -X POST localhost:8080/posts \
       -d "{\"author_id\":\"$u\",\"content\":\"probe\"}")
     [ "$code" = "502" ] && echo "$u: 502 — author shard is the down one"
   done
   docker start "$shard" && sleep 10    # heartbeats clear, queue drains, writes resume
   ```
   Or just run `bash scripts/demo/run.sh` — it narrates this whole drill
   (Part E) with live output.

### 5.4 Production build

```bash
cd frontend
npm run build          # outputs dist/ (static, API calls still go to /api)
npm run preview        # serves dist/ for a quick check
```

The `/api` base keeps the production build able to sit behind any proxy that
forwards `/api` to the coordinator.

### Presenting without a cluster

`demo/simulation.html` animates all of the above (ring lookups, fan-out, replication, heartbeat failover) in the browser with narration, and needs nothing running. Use it for walkthroughs; use `scripts/demo/run.sh` when you need to show the real cluster.

---

## 6. Operations

```bash
# Logs
docker-compose logs -f coordinator      # watch routing + fan-out decisions
docker-compose logs -f shard1          # watch replication commits

# Full reset (wipes all data volumes — cleanest way to re-demo)
docker-compose down -v
# if using cluster.sh:
docker ps -q --filter "name=social_feed" | xargs docker rm -f
docker volume ls --filter "name=social_feed" -q | xargs docker volume rm -f
# then re-run Section 3 from step 1.

# Stop (keep data)
docker-compose stop

# Cluster status
curl localhost:8080/cluster

# Raw API without the frontend
curl -X POST localhost:8080/users -d '{"id":"dave","name":"Dave"}'
curl -X POST localhost:8080/follow -d '{"follower":"dave","followee":"alice"}'
curl -X POST localhost:8080/posts -d '{"author_id":"dave","content":"hi from dave"}'
curl "localhost:8080/feed/dave"
curl localhost:8080/users/dave            # {id, name, follower_count} — 404 if unknown
curl "localhost:8080/users/dave/following"
```

---

## 7. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `docker-compose up` → `ContainerConfig KeyError` | compose v1 vs. Docker 29; use `bash scripts/cluster.sh` |
| `curl localhost:8080/cluster` shows `reachable: false` | heartbeat grace period (~15s); wait, or the node crashed — check `docker-compose logs` |
| Empty feeds right after seeding | order matters (no backfill): follows must precede posts — reseed or reset |
| Feed shows duplicates | reseeded over existing data; do `docker-compose down -v` + full reseed |
| Frontend can't reach backend | Vite proxies `/api` → `localhost:8080`; is the coordinator up? Is the dev server running from `frontend/`? |
| Posting as a user → 502 | their shard is down (by design: posts are never queued) |
| Everything works but slow | first seed creates 1001 users; it's concurrent (50 workers) and should take ~2s |
