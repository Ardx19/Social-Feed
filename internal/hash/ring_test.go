package hash

import "testing"

func TestGetNodeDeterministic(t *testing.T) {
	r := NewRing(3)
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("c")
	first, err := r.GetNode("user123")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		got, _ := r.GetNode("user123")
		if got != first {
			t.Fatalf("GetNode not deterministic: %q then %q", first, got)
		}
	}
}

func TestEmptyRingErrors(t *testing.T) {
	r := NewRing(3)
	if _, err := r.GetNode("x"); err == nil {
		t.Fatal("expected error on empty ring")
	}
}

func TestSingleNodeGetsEverything(t *testing.T) {
	r := NewRing(3)
	r.AddNode("only")
	for i := 0; i < 100; i++ {
		got, err := r.GetNode("key" + string(rune(i)))
		if err != nil {
			t.Fatal(err)
		}
		if got != "only" {
			t.Fatalf("single-node ring routed %q to %q", "key"+string(rune(i)), got)
		}
	}
}

func TestDistributionThreeNodes(t *testing.T) {
	r := NewRing(150)
	r.AddNode("s1")
	r.AddNode("s2")
	r.AddNode("s3")

	counts := map[string]int{}
	const n = 10000
	for i := 0; i < n; i++ {
		got, err := r.GetNode("key" + string(rune(i)))
		if err != nil {
			t.Fatal(err)
		}
		counts[got]++
	}
	for _, node := range []string{"s1", "s2", "s3"} {
		c := counts[node]
		if c == 0 {
			t.Fatalf("node %s got no keys", node)
		}
		// 10000 keys across 3 nodes -> ~3333 each; with 150 virtual nodes,
		// expect each well above 25% (2500). Looser bound catches pathological rings.
		if c < 2500 {
			t.Errorf("node %s share = %d (%.1f%%), want >= 25%%", node, c, 100*float64(c)/n)
		}
	}
}

func TestRemoveNodeConsistency(t *testing.T) {
	r := NewRing(150)
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("c")

	keys := make([]string, 2000)
	before := make(map[string]string, len(keys))
	for i := range keys {
		keys[i] = "k" + string(rune(i))
		before[keys[i]], _ = r.GetNode(keys[i])
	}

	r.RemoveNode("b")

	for _, k := range keys {
		now, _ := r.GetNode(k)
		prev := before[k]
		if prev == "b" {
			// Was on b: must now map to a or c (never b).
			if now == "b" {
				t.Fatalf("key %q still maps to removed node b", k)
			}
		} else {
			// Not on b: must be untouched (consistent hashing property).
			if now != prev {
				t.Fatalf("key %q moved %q -> %q after removing an unrelated node", k, prev, now)
			}
		}
	}
}

func TestBoundaryResolves(t *testing.T) {
	// GetNode on an exact virtual-node hash must still return a valid node.
	r := NewRing(3)
	r.AddNode("a")
	r.AddNode("b")
	// Hash a key and, if its position collides with a virtual node, ensure no panic.
	got, err := r.GetNode("boundary-key")
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Fatal("boundary returned empty node")
	}
}
