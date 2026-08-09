package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Error classification between the coordinator and shards. The coordinator
// distinguishes three outcomes: missing (200 + null), shard-error (5xx), and
// down (connection failure / timeout). This taxonomy drives failover and
// queueing decisions in U8/U10. See design.md.
var (
	// ErrDown means the shard is unreachable (connection refused or timeout).
	ErrDown = errors.New("shard down")
	// ErrShard means the shard returned a 5xx.
	ErrShard = errors.New("shard error")
	// ErrNotFound means the shard reported the entity is absent (404).
	ErrNotFound = errors.New("not found")
	// ErrConflict means the shard reported a conflicting state (409) — e.g. a
	// duplicate user creation.
	ErrConflict = errors.New("conflict")
)

// ShardClient talks to shard nodes over HTTP. Keys are ASCII strings; values
// are opaque bytes, base64-encoded for transport. The coordinator encodes
// values via the shared codec before calling the client — the client never
// touches the codec.
type ShardClient struct {
	http    *http.Client
	timeout time.Duration
}

// NewShardClient builds a client with a short per-request timeout so a hung
// shard is classified as ErrDown rather than stalling the coordinator.
func NewShardClient(timeout time.Duration) *ShardClient {
	return &ShardClient{
		http:    &http.Client{Timeout: timeout},
		timeout: timeout,
	}
}

// Pair is a decoded key/value result from a scan.
type Pair struct {
	Key   string
	Value []byte
}

// classify turns an HTTP response/error into the error taxonomy.
func classify(err error, statusCode int, body string) error {
	if err != nil {
		// Connection refused, timeout, or transport error => down.
		return fmt.Errorf("%w: %v", ErrDown, err)
	}
	if statusCode >= 500 {
		return fmt.Errorf("%w: status %d: %s", ErrShard, statusCode, strings.TrimSpace(body))
	}
	if statusCode == 404 {
		return ErrNotFound
	}
	return nil
}

func (c *ShardClient) doJSON(method, url string, body any, out any) (int, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return 0, err
		}
	}
	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, ErrDown
	}
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	if out != nil && resp.StatusCode == http.StatusOK {
		if derr := dec.Decode(out); derr != nil {
			return resp.StatusCode, derr
		}
	}
	return resp.StatusCode, nil
}

// Write sends a single batch of key/value pairs to a shard (one txn on the shard).
func (c *ShardClient) Write(addr string, pairs []byteKV) error {
	code, err := c.doJSON(http.MethodPost, "http://"+addr+"/write", map[string]any{"pairs": pairs}, nil)
	if err != nil {
		return err
	}
	return classify(nil, code, "")
}

// Read fetches a raw value. found is false for an absent key (the missing case,
// which is NOT an error — the coordinator must distinguish empty from failed).
func (c *ShardClient) Read(addr, key string) ([]byte, bool, error) {
	var out struct {
		Value *string `json:"value"`
	}
	url := "http://" + addr + "/read?key=" + url.QueryEscape(key)
	code, err := c.doJSON(http.MethodGet, url, nil, &out)
	if err != nil {
		return nil, false, err
	}
	if code == http.StatusOK {
		if out.Value == nil {
			return nil, false, nil // missing
		}
		b, derr := base64.StdEncoding.DecodeString(*out.Value)
		if derr != nil {
			return nil, false, derr
		}
		return b, true, nil
	}
	return nil, false, classify(nil, code, "")
}

// Scan fetches key/value pairs under a prefix. reverse returns newest-first.
func (c *ShardClient) Scan(addr, prefix string, limit int, reverse bool) ([]Pair, error) {
	return c.scan(addr, prefix, "", limit, reverse)
}

// ScanFrom fetches key/value pairs under a prefix in reverse, starting from
// the cursor seek key (exclusive). Used by feed pagination.
func (c *ShardClient) ScanFrom(addr, prefix, from string, limit int) ([]Pair, error) {
	return c.scan(addr, prefix, from, limit, true)
}

func (c *ShardClient) scan(addr, prefix, from string, limit int, reverse bool) ([]Pair, error) {
	var out struct {
		Entries []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"entries"`
	}
	u := "http://" + addr + "/scan?prefix=" + url.QueryEscape(prefix) + fmt.Sprintf("&limit=%d&reverse=%v", limit, reverse)
	if from != "" {
		u += "&from=" + url.QueryEscape(from)
	}
	code, err := c.doJSON(http.MethodGet, u, nil, &out)
	if err != nil {
		return nil, err
	}
	if err := classify(nil, code, ""); err != nil {
		return nil, err
	}
	pairs := make([]Pair, 0, len(out.Entries))
	for _, e := range out.Entries {
		var val []byte
		if e.Value != "" {
			var derr error
			val, derr = base64.StdEncoding.DecodeString(e.Value)
			if derr != nil {
				return nil, derr
			}
		}
		pairs = append(pairs, Pair{Key: e.Key, Value: val})
	}
	return pairs, nil
}

// Delete removes a key on a shard.
func (c *ShardClient) Delete(addr, key string) error {
	req, err := http.NewRequest(http.MethodDelete, "http://"+addr+"/delete?key="+url.QueryEscape(key), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return ErrDown
	}
	resp.Body.Close()
	if resp.StatusCode >= 500 {
		return ErrShard
	}
	return nil
}

// FollowEdge / UnfollowEdge call the count-bearing graph endpoint on a shard.
func (c *ShardClient) FollowEdge(addr, followee, follower string) error {
	return c.edge("follow-edge", addr, followee, follower)
}

func (c *ShardClient) UnfollowEdge(addr, followee, follower string) error {
	return c.edge("unfollow-edge", addr, followee, follower)
}

func (c *ShardClient) edge(kind, addr, followee, follower string) error {
	body := map[string]string{"followee": followee, "follower": follower}
	code, err := c.doJSON(http.MethodPost, "http://"+addr+"/"+kind, body, nil)
	if err != nil {
		return err
	}
	return classify(nil, code, "")
}

// CreateUser creates a user (with self-follow edges) on the owning shard.
// Returns ErrConflict if the user already exists, ErrDown/ErrShard on failure.
func (c *ShardClient) CreateUser(addr, id, name string) error {
	code, err := c.doJSON(http.MethodPost, "http://"+addr+"/create-user", map[string]string{"id": id, "name": name}, nil)
	if err != nil {
		return err
	}
	if code == http.StatusConflict {
		return ErrConflict
	}
	return classify(nil, code, "")
}

// Health pings a node; ok is false when it is unreachable or unhealthy.
func (c *ShardClient) Health(addr string) (bool, error) {
	code, err := c.doJSON(http.MethodGet, "http://"+addr+"/health", nil, &struct{}{})
	if err != nil {
		return false, err
	}
	return code == http.StatusOK, classify(nil, code, "")
}

// byteKV is the JSON wire form of a key/value pair (value base64).
type byteKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
