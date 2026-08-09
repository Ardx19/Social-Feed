# Phase 1 — Single Node Works

> Learning target: an embedded storage engine, careful key design, and a working shard node over HTTP — before any distributed complexity.

## What we built

The foundation every other phase builds on: the Go module, the four model structs, the keys package (the single home of every key shape), a shared msgpack+zstd codec, and a BadgerDB storage wrapper that also owns the conditional count-bearing graph transactions. No networking yet — this phase is the data layer only (the HTTP server lands in Phase 2/U2).

## Concepts learned

- **Embedded key-value storage:** BadgerDB is a pure-Go LSM tree with efficient range scans, embedded in the process. No server to run — each shard owns its DB.
- **Key-value vs relational:** no schema, no joins. Relationships are expressed through key design — "who follows X" is a prefix scan.
- **Lexicographic key ordering:** keys sort byte-wise. This is the whole game — design keys so that the order you want to read them in equals their byte order.
- **Range/prefix scans:** `posts_by_author:{author}:*` returns all of one author's posts as one ordered scan — the primitive everything else builds on.
- **Compression at rest, measured:** zstd frames have ~10+ bytes overhead, so compressing a tiny Edge inflates it. We skip compression below 64 bytes with a 1-byte flag.
- **Optimistic concurrency (BadgerDB):** write transactions conflict if they touch a key another committed txn changed; the DB hands you `ErrConflict` and you must retry. This is how concurrent followers on a celebrity serialize.

## Key decisions + why

| Decision | Why | Rejected alternative |
|----------|-----|----------------------|
| BadgerDB (embedded, pure Go) | Range scans, no server to run, LSM inline values | SQLite, pure in-memory |
| Keys owned by a `keys` package | Single place to define/assemble key shapes | Hand-assembling keys in handlers |
| Zero-padded 19-char ns timestamps | Lexicographic sort == chronological sort | Variable-length timestamps |
| Fixed-width post IDs (`crypto/rand` hex) | Same invariant as timestamps: IDs sort lexicographically inside a timestamp slot | Variable-length IDs (interleave wrong) |
| `:{post_id}` suffix on feed/author keys | Two posts in the same ns slot would overwrite each other | Pure timestamp keys |
| Author index `posts_by_author` | Celebrity read path needs single-shard scan by author; `posts:{id}` hashes across shards | Scanning all posts |
| msgpack + zstd — measured, not assumed | zstd frames carry ~10+ bytes overhead; tiny values inflate. Skip below 64 bytes with a 1-byte raw/compressed flag | Blind zstd on everything |
| Codec in `internal/codec` (shared) | Both coordinator (to encode fan-out bytes) and storage (count-bearing txn) need it; no leak into handlers | Codec inside handlers |
| Conditional count-bearing txn in storage | Edge check + count change in ONE txn, retried on conflict — the count is the celebrity gate | Generic increment endpoint; txn in the api handler |

## Structure map

- `internal/models` — User, Post, FeedEntry, Edge structs; `Post.ToFeedEntry` denormalizes content
- `internal/keys` — every key shape (build + prefix helpers); `NewID` fixed-width hex
- `internal/codec` — msgpack + zstd with the raw/compressed flag byte; threshold 64 bytes
- `internal/storage` — BadgerDB wrapper (Get/Set/Delete/Scan/ScanKeys/Batch) + `CreateUser` + conditional `AddFollower`/`RemoveFollower` with conflict retry

## Gotchas & edge cases

- **Reverse prefix scans:** BadgerDB's reverse iterator seeks backward from the seek key, so `Seek(prefix)` lands *before* all keys in the prefix → empty result. Fix: seek to `prefix + 0xFF` (a high bound), then step backward while still inside the prefix. This was a real bug caught by a test.
- **`db.Update` does not retry `ErrConflict`:** with a hot key (a celebrity's user record), N concurrent follows need ~N serialized commits, one winner per round. A 10-retry cap lost ~half of them. Fix: generous cap (1000) + capped exponential backoff. The concurrent-follow test is really testing this retry loop, not Badger atomicity.
- **Self-edge lives in the follower edge set:** `CreateUser` writes `graph:followers:{u}:{u}` too, so follower-scans always include the user themselves. FollowerCount correctly excludes self, but edge scans don't — tests must account for it.

## Interview talking points

- "Why BadgerDB?" — embedded, pure Go, LSM + range scans; values under 1MB live in the LSM tree (no value-log churn for our data).
- "Why does key design matter?" — keys sort lexicographically, so key shape decides what a single ordered scan can return. The `{ts19}:{post_id}` suffixing is the same invariant twice: fixed-width so byte order == logical order, and it prevents overwrite collisions.
- "Why the author index?" — `posts:{id}` hashes across shards by post ID, so it can't answer "recent posts by author X". The index makes it a single-shard range scan.
- "How do you handle concurrent follows?" — optimistic concurrency: read-modify-write the count in one transaction, retry `ErrConflict` with backoff. A celebrity is a hot key, so every follow serializes through the retry loop — accepted at this scale; a separate count key is the future decoupling.
