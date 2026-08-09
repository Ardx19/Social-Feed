package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func trimHost(url string) string { return strings.TrimPrefix(url, "http://") }

func TestMonitorMissesThenDownAndRecovers(t *testing.T) {
	// A fake shard whose health we toggle.
	var up atomic.Bool
	up.Store(true)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !up.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()
	addr := trimHost(ts.URL)

	client := NewShardClient(2 * 1000 * 1000 * 1000)
	m := NewHealthMonitor(client, map[string]string{"s1": addr}, map[string]string{}, 0, 2)

	// Two healthy checks keep it up.
	m.checkOnce()
	m.checkOnce()
	if m.IsDown("s1") {
		t.Fatal("node should be up after healthy pings")
	}

	// Take it down: two misses (threshold) -> down.
	up.Store(false)
	m.checkOnce()
	if m.IsDown("s1") {
		t.Fatal("should not be down after one miss (threshold 2)")
	}
	m.checkOnce()
	if !m.IsDown("s1") {
		t.Fatal("node should be down after two misses")
	}

	// Recovery fires onRecover.
	var recovered atomic.Bool
	m.SetOnRecover(func(nodeID string) {
		if nodeID == "s1" {
			recovered.Store(true)
		}
	})
	up.Store(true)
	m.checkOnce()
	if m.IsDown("s1") {
		t.Fatal("node should be back up after a healthy ping")
	}
	if !recovered.Load() {
		t.Fatal("onRecover not fired on recovery")
	}
}

func TestMonitorReplicaFor(t *testing.T) {
	client := NewShardClient(1000)
	m := NewHealthMonitor(client, map[string]string{"s1": "a:1"}, map[string]string{"s1": "r:4"}, 0, 2)
	if rep, ok := m.ReplicaFor("s1"); !ok || rep != "r:4" {
		t.Fatalf("ReplicaFor = %q,%v want r:4,true", rep, ok)
	}
	if _, ok := m.ReplicaFor("missing"); ok {
		t.Fatal("ReplicaFor for unknown node should be false")
	}
}
