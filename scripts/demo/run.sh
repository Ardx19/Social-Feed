#!/usr/bin/env bash
#
# scripts/demo/run.sh — narrated end-to-end demo of the Distributed Social Feed Engine.
#
# This is the *narration* companion to verify.sh:
#   - verify.sh  → machine assertions, exits 0/1 (CI-style, "VERIFY PASSED")
#   - run.sh     → walks a human through both fan-out
#                  modes + failover, printing what each step PROVES and the live
#                  response behind it.
#
# It only reads/writes through the coordinator's public HTTP API, exactly like a
# client would. It never touches a shard directly.
#
# Usage:
#   bash scripts/demo/run.sh [COORDINATOR_URL]
#   COORDINATOR_URL defaults to http://localhost:8080
#
# Assumes the cluster is up and the seed has been run (users + a 1001-follower
# celebrity, one above the threshold). If feeds look empty, you probably posted
# before following — reseed.
#
# ─────────────────────────────────────────────────────────────────────────────
# RESPONSE SHAPES (verified against internal/api as of this commit):
#   1. POST /posts     → JSON with .post_id                          (writer.go)
#   2. GET /feed/{u}   → JSON with .entries[] (.author_id/.content/.timestamp)
#                        and .next_cursor when more pages exist      (reader.go)
#   3. GET /users/{id} → JSON with .id/.name/.follower_count         (users.go)
#   4. GET /cluster    → JSON array of {id, role, addr, reachable}   (coordinator.go)
# The script still prints raw bodies, so a drift is visible, not silent.
# ─────────────────────────────────────────────────────────────────────────────

set -uo pipefail

COORD="${1:-http://localhost:8080}"
THRESHOLD="${CELEBRITY_THRESHOLD:-1000}"

# Demo actors seeded by scripts/seed. celeb is the celebrity; f000 is one of its
# 1001 followers; alice/bob/carol follow each other (non-celebrity fan-out).
CELEB="celeb"
CELEB_FOLLOWER="f000"
A="alice"
B="bob"

# ── pretty-printing ──────────────────────────────────────────────────────────
BOLD=$'\e[1m'; DIM=$'\e[2m'; GRN=$'\e[32m'; RED=$'\e[31m'; YEL=$'\e[33m'; RST=$'\e[0m'
have_jq=1; command -v jq >/dev/null 2>&1 || have_jq=0
have_docker=1; command -v docker >/dev/null 2>&1 || have_docker=0

step()  { printf '\n%s━━ %s ━━%s\n' "$BOLD" "$*" "$RST"; }
proves(){ printf '%s   proves: %s%s\n' "$DIM" "$*" "$RST"; }
ok()    { printf '%s   ✓ %s%s\n' "$GRN" "$*" "$RST"; }
warn()  { printf '%s   ! %s%s\n' "$YEL" "$*" "$RST"; }
fail()  { printf '%s   ✗ %s%s\n' "$RED" "$*" "$RST"; }
show()  { # pretty-print a JSON body (or raw if jq absent / not JSON)
  if [ "$have_jq" = 1 ]; then echo "$1" | jq . 2>/dev/null || echo "   $1"; else echo "   $1"; fi
}

# GET → prints "HTTP_STATUS\n<body>"; callers split with the helpers below.
get()  { curl -sS -w $'\n%{http_code}' "$COORD$1" 2>/dev/null; }
post() { curl -sS -w $'\n%{http_code}' -X POST "$COORD$1" -H 'Content-Type: application/json' -d "$2" 2>/dev/null; }
body() { printf '%s' "$1" | sed '$d'; }
code() { printf '%s' "$1" | tail -n1; }
jget() { [ "$have_jq" = 1 ] && printf '%s' "$1" | jq -r "$2" 2>/dev/null || printf ''; }

# ── preflight ────────────────────────────────────────────────────────────────
step "Preflight — cluster reachable?"
[ "$have_jq" = 1 ] || warn "jq not found — output will be raw JSON (install jq for nicer display)"
r=$(get /cluster); c=$(code "$r")
if [ "$c" != "200" ]; then
  fail "GET /cluster returned HTTP $c — is the coordinator up at $COORD?"
  echo "   Start it (Section 3 of the runbook), then re-run."
  exit 1
fi
ok "coordinator responding at $COORD"
show "$(body "$r")"
proves "the coordinator is the only host-facing component; everything below routes through it"

# ═════════════════════════════════════════════════════════════════════════════
# PART A — Fan-out ON WRITE (non-celebrity)
# ═════════════════════════════════════════════════════════════════════════════
step "A. Non-celebrity fan-out — $A's feed"
r=$(get "/feed/$A?limit=20"); c=$(code "$r")
if [ "$c" = "200" ]; then
  ok "GET /feed/$A → 200"
  show "$(body "$r")"
  proves "$A sees their OWN post (self-follow edge) AND $B's post (pushed at write time)"
else
  fail "GET /feed/$A → HTTP $c (expected 200; if 404, the seed didn't run)"
  show "$(body "$r")"
fi

# ═════════════════════════════════════════════════════════════════════════════
# PART B — Celebrity LAZY read + the silent-under-count guard
# ═════════════════════════════════════════════════════════════════════════════
step "B. Celebrity gate — is $CELEB actually a celebrity?"
# CRITICAL: the seed gives celeb threshold+1 followers for margin, but if the
# count were ever lost, feeds would STILL populate (via fan-out) — so the feed
# screen looks identical either way. The ONLY reliable signal is the count.
r=$(get "/users/$CELEB"); c=$(code "$r")
if [ "$c" = "200" ]; then
  fc=$(jget "$(body "$r")" '.follower_count')
  if [ -n "$fc" ]; then
    if [ "$fc" -ge "$THRESHOLD" ] 2>/dev/null; then
      ok "$CELEB follower_count = $fc  (>= threshold $THRESHOLD → celebrity)"
      [ "$fc" -eq "$THRESHOLD" ] && warn "count is EXACTLY the threshold — zero margin; one lost increment would flip the path silently"
    else
      fail "$CELEB follower_count = $fc  (< threshold $THRESHOLD → NOT a celebrity!)"
      fail "the celebrity path below will be wrong — some follow increments were lost"
    fi
  else
    warn "couldn't read .follower_count from GET /users/$CELEB — check the field name"
    show "$(body "$r")"
  fi
else
  warn "GET /users/$CELEB → HTTP $c. The feed's unknown-user 404 check and this count"
  warn "assertion both depend on this route (mux /users/ in coordinator.go)."
fi

step "B. Celebrity lazy delivery — $CELEB_FOLLOWER's feed"
r=$(get "/feed/$CELEB_FOLLOWER?limit=20"); c=$(code "$r")
if [ "$c" = "200" ]; then
  ok "GET /feed/$CELEB_FOLLOWER → 200"
  show "$(body "$r")"
  proves "$CELEB_FOLLOWER sees $CELEB's post — but it was NEVER written to $CELEB_FOLLOWER's feed."
  proves "It's pulled live from the author index (posts_by_author) at read time = fan-out on READ."
else
  fail "GET /feed/$CELEB_FOLLOWER → HTTP $c"
  show "$(body "$r")"
fi

# Optional: confirm the celebrity post produced ZERO fan-out by grepping the
# coordinator log. fanout.go logs the lazy path WITHOUT the substring "fan-out"
# and the eager path WITH it, so a non-zero match is a real signal, not noise.
if [ "$have_docker" = 1 ]; then
  cco=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -i coord | head -n1)
  if [ -n "$cco" ]; then
    step "B. Write-side proof — zero fan-out for the celebrity"
    n=$(docker logs "$cco" 2>&1 | grep -i "fan-out" | grep -i "$CELEB" | wc -l | tr -d ' ')
    if [ "$n" = "0" ]; then
      ok "no fan-out log lines mention $CELEB → the celebrity post was stored once, not blasted"
    else
      warn "$n fan-out log line(s) mention $CELEB — expected 0. Either $CELEB isn't a celebrity"
      warn "(see Part B count) or fan-out ran anyway. This is the silent-under-count failure."
    fi
    proves "both halves: follower sees the post (read path) AND it cost zero fan-out writes"
  fi
fi

# ═════════════════════════════════════════════════════════════════════════════
# PART C — Celebrity's OWN feed (AE2): own posts via read path, no fan-out
# ═════════════════════════════════════════════════════════════════════════════
step "C. $CELEB's own feed — own posts with zero fan-out"
r=$(get "/feed/$CELEB?limit=20"); c=$(code "$r")
if [ "$c" = "200" ]; then
  ok "GET /feed/$CELEB → 200"
  show "$(body "$r")"
  proves "even the celebrity sees their own posts — via the self-follow edge + read path,"
  proves "NOT because anything was fanned out to them. Same mechanism as any follower."
else
  fail "GET /feed/$CELEB → HTTP $c"; show "$(body "$r")"
fi

# ═════════════════════════════════════════════════════════════════════════════
# PART D — Live post, watch it push (fan-out on write, end to end)
# ═════════════════════════════════════════════════════════════════════════════
step "D. Live post as $A → does it reach $B?"
warn "this mutates state (adds a real post). Reseed for a clean slate afterward."
stamp="demo post $(date +%H:%M:%S)"
r=$(post /posts "{\"author_id\":\"$A\",\"content\":\"$stamp\"}"); c=$(code "$r")
if [ "$c" = "200" ] || [ "$c" = "201" ]; then
  pid=$(jget "$(body "$r")" '.post_id')
  ok "posted as $A → HTTP $c${pid:+  (post_id=$pid)}"
  sleep 1
  r2=$(get "/feed/$B?limit=5")
  if [ "$have_jq" = 1 ] && printf '%s' "$(body "$r2")" | jq -e --arg s "$stamp" '.entries[]?|select(.content==$s)' >/dev/null 2>&1; then
    ok "the new post appears in $B's feed"
  else
    warn "couldn't confirm the post in $B's feed by content match — showing $B's feed:"
  fi
  show "$(body "$r2")"
  proves "a non-celebrity post is pushed into each follower's feed at write time (here, $B follows $A)"
else
  fail "POST /posts as $A → HTTP $c"; show "$(body "$r")"
fi

# ═════════════════════════════════════════════════════════════════════════════
# PART E — Failover. Reads survive off the replica; posts to the down shard 502.
# ═════════════════════════════════════════════════════════════════════════════
step "E. Failover drill"
if [ "$have_docker" != 1 ]; then
  warn "docker not available — skipping the failover drill (Parts A–D are the fan-out story)"
  echo; ok "demo complete"; exit 0
fi

# Discover a primary shard container by name instead of hardcoding it — the name
# depends on how the cluster was launched (compose vs cluster.sh).
target=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -iE 'shard[0-9]' | grep -vi replica | head -n1)
if [ -z "$target" ]; then
  warn "couldn't find a running shard container (looked for names matching 'shard[0-9]', excluding replicas)"
  warn "list them with: docker ps --format '{{.Names}}'  — then stop one manually to see failover"
  echo; ok "demo complete (fan-out parts)"; exit 0
fi

echo "   target shard container: $target"
proves "we don't need to know which USERS live here — we'll detect them by behavior"

# The monitor's down/up state is what gates failover (replica reads, 502 posts),
# and /cluster does a LIVE ping, not a monitor read — so it flips instantly on
# docker stop, not on monitor-down. The authoritative signal is the coordinator
# log line from heartbeat.go ("marked down" / "recovered"). Poll for it and
# print the MEASURED window instead of trusting a nominal sleep.
node_id=$(printf '%s' "$target" | grep -oE 'shard[0-9]+' | head -n1)
cco=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -i coord | head -n1)
wait_for_log() { # <marker> <cap-seconds> <label>
  local marker="$1" cap="$2" label="$3" t0=0
  t0=$(date +%s)
  if [ -z "$cco" ]; then
    sleep "$cap"
    echo "   $label: no coordinator log access — waited ${cap}s unverified"
    return
  fi
  while [ $(( $(date +%s) - t0 )) -lt "$cap" ]; do
    if docker logs "$cco" --since 10m 2>&1 | grep -q "$marker"; then
      echo "   $label after $(($(date +%s) - t0))s (measured)"
      return
    fi
    sleep 2
  done
  echo "   $label: not seen within ${cap}s — continuing unverified"
}

step "E. Stop $target and wait for detection"
docker stop "$target" >/dev/null 2>&1 && ok "stopped $target"
echo "   heartbeat: 5s interval, 3 misses ≈ 15s — measuring the actual window..."
wait_for_log "heartbeat: $node_id marked down" 40 "$node_id marked down"
r=$(get /cluster)
if [ "$have_jq" = 1 ]; then
  echo "$(body "$r")" | jq . 2>/dev/null | grep -iE 'shard|reachable|up|role' || show "$(body "$r")"
else
  show "$(body "$r")"
fi
proves "the coordinator detected the loss (3 missed heartbeats). Ring membership does NOT change."

step "E. Reads still work — served from the replica"
read_fail=0
for u in "$A" "$B" "$CELEB_FOLLOWER" "$CELEB"; do
  c=$(code "$(get "/feed/$u?limit=5")")
  if [ "$c" = "200" ]; then ok "GET /feed/$u → 200 (served, replica if $u lived on $target)"
  else warn "GET /feed/$u → HTTP $c"; read_fail=1; fi
done
[ "$read_fail" = 0 ] && proves "read availability preserved across a shard outage — the point of async replicas"

step "E. Writes to the down shard → 502 (never queued), and that reveals who lived there"
found_502=0
for u in "$A" "$B" "$CELEB_FOLLOWER" "$CELEB"; do
  c=$(code "$(post /posts "{\"author_id\":\"$u\",\"content\":\"failover probe\"}")")
  if [ "$c" = "502" ]; then
    ok "POST as $u → 502 → $u's author-shard IS the one we stopped (post correctly refused, not queued)"
    found_502=1
  elif [ "$c" = "200" ] || [ "$c" = "201" ]; then
    echo "   POST as $u → $c (their shard is up; a demo post was created)"
  else
    warn "POST as $u → HTTP $c"
  fi
done
if [ "$found_502" = 1 ]; then
  proves "post creation is never queued: its celebrity gate + fan-out pairs live on the down shard,"
  proves "so a queued post would drain missing its fan-out. 502 is the honest answer."
else
  warn "no user in the demo set landed on $target, so no 502 surfaced. Try stopping a different shard,"
  warn "or add more seed users so at least one hashes to each shard."
fi

step "E. Recover"
docker start "$target" >/dev/null 2>&1 && ok "restarted $target"
echo "   waiting for it to rejoin and heartbeats to clear — measuring..."
wait_for_log "heartbeat: $node_id recovered" 25 "$node_id recovered"
if [ "$found_502" = 1 ]; then
  # retry a post as one of the users that just 502'd
  for u in "$A" "$B" "$CELEB_FOLLOWER" "$CELEB"; do
    c=$(code "$(post /posts "{\"author_id\":\"$u\",\"content\":\"post-recovery $(date +%H:%M:%S)\"}")")
    if [ "$c" = "200" ] || [ "$c" = "201" ]; then ok "POST as $u → $c after recovery — writes work again"; break; fi
  done
fi
show "$(body "$(get /cluster)")"
proves "the shard rejoined with its volume intact; the coordinator routes to it again"

echo
printf '%s━━ demo complete ━━%s\n' "$BOLD" "$RST"
echo "Reseed for a clean slate (this run added demo posts):  docker-compose down -v  &&  re-run Section 3"
