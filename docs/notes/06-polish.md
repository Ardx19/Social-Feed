# Phase 6 — Polish

> Learning target: a demoable product — the three-view frontend, seed data, verification, and the design document that ties it all together.

## What we built

*(filled in after the phase)*

## Concepts learned

*(three-view UI against a distributed backend, seed data ordering, demo scripting, turning a build into an interview narrative)*

## Key decisions + why

| Decision | Why | Rejected alternative |
|----------|-----|----------------------|
| Exactly three views, plain React + `useState` + fetch | Enough to demo, nothing more | Router / state library |
| Seed order: create → follow → then post | No backfill; post-then-follow yields empty feeds | Posting first |
| Seed a real 1000-follower celebrity | Both fan-out modes observable in one demo | Only small accounts |
| `design.md` written continuously, assembled here | Reasoning is fresh when written; final pass for interview readiness | Backfill at the end |

## Structure map

- `frontend/` — Feed, Post, Follow views
- `scripts/seed` — seed data generator (celebrity crossing)
- `scripts/demo` — verification + resilience drill
- `README.md` — runbook
- `design.md` — the interview deliverable

## Gotchas & edge cases

*(empty-feed UX vs "user not found", double-submit, load-more cursor state, view timeout vs coordinator retry chain)*

## Interview talking points

*(the threshold-crossing demo, the failover demo, and every "what breaks at scale" answer)*
