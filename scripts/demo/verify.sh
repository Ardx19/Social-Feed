#!/usr/bin/env bash
# Verifies the demo claims against the running cluster (origin AEs AE1-AE3).
# Run after scripts/seed against a fresh cluster.
set -euo pipefail

BASE="${1:-http://localhost:8080}"
EXPECT_FOLLOWERS="${EXPECT_FOLLOWERS:-1000}"
FAIL=0

# Feed entries "author|content" for a user, newest first.
feed_contents() {
  curl -s "$BASE/feed/$1?limit=50" | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    d = {"entries": []}
for e in d.get("entries", []):
    print(e["author_id"] + "|" + e["content"])
'
}

contains() { # needle haystack...
  local needle="$1"; shift
  printf '%s\n' "$@" | grep -Fq "$needle"
}

check() {
  local name="$1"; shift
  if "$@"; then
    echo "PASS  $name"
  else
    echo "FAIL  $name"
    FAIL=1
  fi
}

celeb_count=$(curl -s "$BASE/users/celeb" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("follower_count", -1))')
check "celeb has exactly $EXPECT_FOLLOWERS real followers" \
  test "$celeb_count" = "$EXPECT_FOLLOWERS"

# AE1: alice sees her own post (self-fan-out) and bob sees it (fan-out).
alice_feed=$(feed_contents alice)
bob_feed=$(feed_contents bob)
check "alice sees her own post" \
  contains "alice|Hello from the distributed feed!" "$alice_feed"
check "bob's feed has alice's post (fan-out)" \
  contains "alice|Hello from the distributed feed!" "$bob_feed"

# AE2: celeb sees their own post via the read path (celebrity, no fan-out).
celeb_feed=$(feed_contents celeb)
check "celeb sees their own post (read path)" \
  contains "celeb|A celebrity post" "$celeb_feed"

# AE3: a follower sees the celebrity post lazily via the author index.
follower_feed=$(feed_contents f000)
check "a follower sees the celebrity post (lazy delivery)" \
  contains "celeb|A celebrity post" "$follower_feed"

echo
if [ "$FAIL" = 1 ]; then
  echo "VERIFY FAILED"
  exit 1
fi
echo "VERIFY PASSED — all fan-out modes demonstrated."
