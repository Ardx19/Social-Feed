// Command seed populates the cluster through the coordinator's public API.
// It creates a small social graph plus a celebrity with N real followers so
// both fan-out modes are observable in the demo (origin R12).
//
// Order matters: create users -> follow -> then post. There is no backfill, so
// posting before following yields empty follower feeds (the demo confusion to
// avoid; see design.md).
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
)

var base = flag.String("coordinator", "http://localhost:8080", "coordinator base URL")
var followers = flag.Int("followers", 1001, "number of real followers for the celebrity (threshold + 1, so a single lost increment can't silently flip the celebrity path)")

func main() {
	flag.Parse()
	client := &http.Client{}

	api := func(method, path string, body any) (int, map[string]any) {
		var buf bytes.Buffer
		if body != nil {
			_ = json.NewEncoder(&buf).Encode(body)
		}
		req, err := http.NewRequest(method, *base+path, &buf)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			log.Fatalf("%s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	must := func(step string, code int, ok int) {
		if code != ok {
			log.Fatalf("%s: status %d, want %d", step, code, ok)
		}
	}

	create := func(id string) {
		code, _ := api("POST", "/users", map[string]any{"id": id, "name": id})
		if code != 200 && code != 409 { // 409 = already exists (idempotent reseed)
			log.Fatalf("create %s: status %d", id, code)
		}
	}
	follow := func(f, e string) {
		code, _ := api("POST", "/follow", map[string]any{"follower": f, "followee": e})
		must("follow "+f+"->"+e, code, 200)
	}
	post := func(author, content string) {
		code, _ := api("POST", "/posts", map[string]any{"author_id": author, "content": content})
		must("post by "+author, code, 200)
	}

	// 1. Small graph.
	create("alice")
	create("bob")
	create("carol")
	create("celeb")
	follow("alice", "bob")
	follow("bob", "alice")
	follow("carol", "alice")

	// 2. Celebrity: N real followers, created + followed concurrently.
	n := *followers
	if n < 1 {
		log.Fatal("followers must be >= 1")
	}
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("f%03d", i)
	}
	runConcurrent(ids, func(id string) { create(id) })
	log.Printf("created %d follower users", n)
	runConcurrent(ids, func(id string) { follow(id, "celeb") })
	log.Printf("%d users follow celeb", n)

	// 3. Post only after the graph is built (no backfill).
	post("alice", "Hello from the distributed feed!")
	post("bob", "This is bob, fanning out to alice's followers.")
	post("celeb", "A celebrity post that fans out to NOBODY — delivered lazily on read.")

	log.Printf("seed complete. coordinator at %s", *base)
	os.Exit(0)
}

// runConcurrent applies fn to each item with a bounded worker pool.
func runConcurrent(items []string, fn func(string)) {
	const workers = 50
	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)
	for _, it := range items {
		wg.Add(1)
		sem <- struct{}{}
		go func(it string) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(it)
		}(it)
	}
	wg.Wait()
}
