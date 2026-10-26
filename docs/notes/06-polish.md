# Phase 6 — Polish

> Learning target: a demoable product — the three-view frontend, seed data, verification, and the design document that ties it all together.

## What we built

The React frontend (Feed/Post/Follow views), a seed script that builds a small graph plus a 1001-follower celebrity, a verification script asserting every fan-out mode, a README runbook, and the completed `design.md`. The whole system runs end-to-end and every demo claim is verified.

## Concepts learned

- **A demo is a verification surface.** The seed + verify scripts turn the abstract claims (hybrid fan-out, celebrity lazy delivery) into observable, repeatable checks.
- **Turning a build into a design document.** The design doc synthesizes each "why" into one coherent account: threshold-crossing, failover, what breaks at scale, what you'd do differently.

## Key decisions + why

| Decision | Why | Rejected alternative |
|----------|-----|----------------------|
| Exactly three views, plain React + `useState` + fetch | Enough to demo, nothing more | Router / state library |
| Seed order: create → follow → then post | No backfill; post-then-follow yields empty feeds | Posting first |
| Seed a real 1001-follower celebrity (threshold + 1) | Both fan-out modes observable in one demo; one margin of safety against a lost follow increment silently flipping the celebrity path | Only small accounts |
| In-flight states disable submit/load | Prevents double-submit (duplicate posts, inflated counts) | Live buttons |
| Follow View uses the real `/users/{id}/following` | Shows actual backend state, self-edge filtered | Local-only guess |
| `design.md` written continuously, assembled here | Reasoning is fresh when written; final pass for completeness | Backfill at the end |

## Structure map

- `frontend/` — Feed, Post, Follow views; Vite dev proxy to the coordinator
- `scripts/seed` — Go seed (1001-follower celebrity, concurrent follows)
- `scripts/demo/verify.sh` — asserts AE1/AE2/AE3 fan-out modes
- `scripts/demo/run.sh` — narrated walkthrough: both fan-out modes + failover drill
- `README.md` — runbook: cluster, seed, frontend, resilience drill
- `design.md` — the design document (complete)

## Gotchas & edge cases

- **Empty-feed UX vs "user not found":** an unknown user must render "user not found" (404), not a misleading "no posts yet" (200 []).
- **Self-edge in the following list:** the Follow View must filter the user's own self-edge — it's a system invariant, not a user-managed follow (and unfollow-self is rejected).
- **Seed concurrency is safe now:** the conditional-transaction + conflict-retry contract makes 1001 concurrent follows converge on exactly 1001.
- **The demo resilience narrative needs the ~15s heartbeat window scripted** (stop → wait → read from replica → post 502 → restart → recovered).
