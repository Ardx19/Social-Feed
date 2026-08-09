
package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"socialfeed/internal/api"
	"socialfeed/internal/codec"
	"socialfeed/internal/replication"
	"socialfeed/internal/storage"
)

// ./app <coordinator|shard>
func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: ./app <coordinator|shard>")
	}
	switch os.Args[1] {
	case "shard":
		runShard()
	case "coordinator":
		runCoordinator()
	default:
		log.Fatalf("unknown subcommand %q", os.Args[1])
	}
}

// runCoordinator starts the coordinator from env:
//   PORT, SHARDS, REPLICAS, CELEBRITY_THRESHOLD
func runCoordinator() {
	port := envOr("PORT", "8080")
	shards := envOr("SHARDS", "")
	replicas := envOr("REPLICAS", "")
	threshold := int64(1000)
	if v := envOr("CELEBRITY_THRESHOLD", ""); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			threshold = n
		}
	}

	if shards == "" {
		log.Fatal("SHARDS env required (e.g. shard1:8081,shard2:8082,shard3:8083)")
	}
	coord := api.NewCoordinator(api.CoordinatorConfig{
		Shards:    api.ParseTopology(shards),
		Replicas:  api.ParseTopology(replicas),
		Threshold: threshold,
		Client:    api.NewShardClient(2 * time.Second),
	})
	addr := ":" + port
	log.Printf("coordinator listening on %s, threshold=%d", addr, threshold)
	if err := http.ListenAndServe(addr, coord.Handler()); err != nil {
		log.Fatal(err)
	}
}

// runShard starts a shard node from env:
//   NODE_ID, PORT, ROLE (default primary), REPLICA (replica addr for primary),
//   DATA_DIR
func runShard() {
	id := envOr("NODE_ID", "shard")
	port := envOr("PORT", "8081")
	role := envOr("ROLE", "primary")
	replica := envOr("REPLICA", "")
	dataDir := envOr("DATA_DIR", "/data/"+id)

	c, err := codec.New()
	if err != nil {
		log.Fatalf("codec: %v", err)
	}
	store, err := storage.Open(dataDir, c)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer store.Close()

	// Only primaries replicate. A replica must NOT construct a replicator —
	// same binary, so this is an explicit startup branch; a replica replicating
	// onward would hammer a non-existent chain (see design.md).
	if role == "primary" && replica != "" {
		rep := replication.NewReplicator(replica, 4096)
		defer rep.Stop()
		store.SetCommitHook(func(ws storage.WriteSet) {
			op := replication.Op{}
			for _, p := range ws.Pairs {
				op.Pairs = append(op.Pairs, replication.Pair{Key: p.Key, Value: p.Value})
			}
			for _, d := range ws.Deletes {
				op.Deletes = append(op.Deletes, string(d))
			}
			rep.Enqueue(op)
		})
		log.Printf("shard %s replicating to %s", id, replica)
	}

	srv := api.NewShard(store, id, role)
	addr := ":" + port
	log.Printf("shard %s (%s) listening on %s, data=%s", id, role, addr, dataDir)
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
