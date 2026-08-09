#!/usr/bin/env bash
# Brings up the 7-service cluster using raw docker (workaround for the
# docker-compose v1 <-> Docker 29 incompatibility in some environments).
# `docker-compose.yml` remains the canonical definition for environments with
# a working compose v2; this script is the drop-in for environments where
# `docker-compose up` fails with a ContainerConfig KeyError.
set -euo pipefail

NET=social_feed_default
IMG=social-feed:latest
cd "$(dirname "$0")/.."

docker network create "$NET" >/dev/null 2>&1 || true

# coordinator
docker rm -f social_feed_coordinator_1 >/dev/null 2>&1 || true
docker run -d --name social_feed_coordinator_1 --network "$NET" -p 8080:8080 \
  -e SHARDS=shard1:8081,shard2:8082,shard3:8083 \
  -e REPLICAS=shard1:replica1:8084,shard2:replica2:8085,shard3:replica3:8086 \
  -e CELEBRITY_THRESHOLD=1000 \
  "$IMG" coordinator >/dev/null

# shards + replicas
for i in 1 2 3; do
  rp=$((i+3))
  docker rm -f "social_feed_shard${i}_1" "social_feed_replica${i}_1" >/dev/null 2>&1 || true
  docker run -d --name "social_feed_shard${i}_1" --network "$NET" --network-alias "shard${i}" \
    -e NODE_ID="shard${i}" -e PORT="808${i}" -e ROLE=primary -e REPLICA="replica${i}:808${rp}" \
    -e DATA_DIR="/data/shard${i}" -v "social_feed_shard${i}-data":/data \
    "$IMG" shard >/dev/null
  docker run -d --name "social_feed_replica${i}_1" --network "$NET" --network-alias "replica${i}" \
    -e NODE_ID="replica${i}" -e PORT="808${rp}" -e ROLE=replica \
    -e DATA_DIR="/data/replica${i}" -v "social_feed_replica${i}-data":/data \
    "$IMG" shard >/dev/null
done

echo "cluster up. coordinator at http://localhost:8080"
echo "nodes:"; curl -s localhost:8080/cluster
echo
