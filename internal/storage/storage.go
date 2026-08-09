package storage

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"

	"socialfeed/internal/codec"
	"socialfeed/internal/keys"
	"socialfeed/internal/models"
)

// ErrNotFound is returned by the count-bearing graph methods when a user
// record is missing. It lets the coordinator surface a 404 without leaking
// BadgerDB's key-not-found error.
var ErrNotFound = errors.New("storage: not found")

// errAlreadyFollowing / errNotFollowing are internal sentinels returned when a
// follow/unfollow is idempotently a no-op. Callers treat them as success.
var (
	errAlreadyFollowing = errors.New("storage: already following")
	errNotFollowing     = errors.New("storage: not following")
)

// maxConflictRetries bounds the optimistic-concurrency retry loop. BadgerDB's
// db.Update does not retry on ErrConflict; concurrent writes to the same user
// record (a celebrity is a hot key) must be retried by the caller, and each
// successful commit clears contention, so the loop converges. Backoff is
// capped so worst-case is bounded. See design.md.
const (
	maxConflictRetries = 1000
	maxBackoff         = 2 * time.Millisecond
	initialBackoff     = 50 * time.Microsecond
)

// Store wraps a BadgerDB instance and the codec. It is the only place that
// touches the DB; callers operate on keys (via the keys package) and values.
// Raw writes carry opaque []byte values (assembled by the coordinator through
// the shared codec); the count-bearing graph methods do their own
// encode/decode so the codec never leaks into API handlers. See design.md.
type Store struct {
	db    *badger.DB
	codec *codec.Codec
	// commitHook, when set (primaries only), is invoked after each successful
	// write transaction with that transaction's FULL write-set. This is what
	// makes replication one-txn -> one-op -> one-txn (see design.md).
	commitHook func(WriteSet)
}

// KV is a raw key/value byte pair.
type KV struct {
	Key   []byte
	Value []byte
}

// WriteSet is one committed transaction's write-set: absolute final values
// (pairs) and deletes. It is the unit of replication — a transaction's whole
// write-set (e.g. follower edge + incremented count together) rides in one op
// so the replica can never observe the pair split. See design.md.
type WriteSet struct {
	Pairs   []KV
	Deletes [][]byte
}

// SetCommitHook installs the post-commit write-set notifier (used by the
// shard to feed a replicator). Primaries only — replicas never set this, so a
// replica never replicates onward.
func (s *Store) SetCommitHook(hook func(WriteSet)) { s.commitHook = hook }

func (s *Store) notify(ws WriteSet) {
	if s.commitHook != nil {
		s.commitHook(ws)
	}
}

// Open opens (or creates) the BadgerDB at path using the given codec.
func Open(path string, c *codec.Codec) (*Store, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", path, err)
	}
	return &Store{db: db, codec: c}, nil
}

// Close flushes and closes the DB.
func (s *Store) Close() error { return s.db.Close() }

// Get returns the raw value for key. found is false when the key is absent.
func (s *Store) Get(key []byte) (value []byte, found bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, gerr := txn.Get(key)
		if gerr == badger.ErrKeyNotFound {
			return nil
		}
		if gerr != nil {
			return gerr
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, false, fmt.Errorf("storage: get: %w", err)
	}
	return value, value != nil, nil
}

// Set writes a single raw key/value pair in one transaction.
func (s *Store) Set(key, value []byte) error {
	return s.Batch([]KV{{Key: key, Value: value}})
}

// Delete removes a key. Deleting an absent key is a no-op.
func (s *Store) Delete(key []byte) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
	if err != nil {
		return fmt.Errorf("storage: delete: %w", err)
	}
	s.notify(WriteSet{Deletes: [][]byte{key}})
	return nil
}

// Batch writes all pairs in ONE transaction (all-or-nothing). Used by the
// shard's batched write endpoint and by fan-out, where a whole shard batch
// must apply atomically or not at all. The whole batch replicates as one op.
func (s *Store) Batch(pairs []KV) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, p := range pairs {
			if err := txn.Set(p.Key, p.Value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("storage: batch: %w", err)
	}
	s.notify(WriteSet{Pairs: pairs})
	return nil
}

// ApplyWriteSet applies a write-set (pairs + deletes) in ONE transaction. Used
// by a replica's /replicate handler. It does NOT notify — a replica never
// replicates onward.
func (s *Store) ApplyWriteSet(ws WriteSet) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, p := range ws.Pairs {
			if err := txn.Set(p.Key, p.Value); err != nil {
				return err
			}
		}
		for _, d := range ws.Deletes {
			if err := txn.Delete(d); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("storage: apply writeset: %w", err)
	}
	return nil
}

// Scan iterates keys under prefix, newest-first when reverse is true, up to
// limit entries (0 = unlimited). Returns raw value bytes in key order.
func (s *Store) Scan(prefix []byte, limit int, reverse bool) ([][]byte, error) {
	var out [][]byte
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			Prefix:  prefix,
			Reverse: reverse,
		})
		defer it.Close()
		// For reverse iteration, seek to a key greater than every key under the
		// prefix (prefix + 0xFF), then step backward while still inside the
		// prefix. Seeking directly to prefix in reverse lands before all keys.
		seek := prefix
		if reverse {
			high := make([]byte, len(prefix), len(prefix)+1)
			copy(high, prefix)
			seek = append(high, 0xff)
		}
		for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			out = append(out, v)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage: scan: %w", err)
	}
	return out, nil
}

// ScanKeys iterates keys under prefix and returns the raw key bytes, newest
// first when reverse is true, up to limit. Used by the read path to collect
// keys without decoding every value.
func (s *Store) ScanKeys(prefix []byte, limit int, reverse bool) ([][]byte, error) {
	var out [][]byte
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			Prefix:  prefix,
			Reverse: reverse,
		})
		defer it.Close()
		seek := prefix
		if reverse {
			high := make([]byte, len(prefix), len(prefix)+1)
			copy(high, prefix)
			seek = append(high, 0xff)
		}
		for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
			k := it.Item().Key()
			kc := make([]byte, len(k))
			copy(kc, k)
			out = append(out, kc)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage: scankeys: %w", err)
	}
	return out, nil
}

// ScanPairs iterates keys under prefix in forward order, up to limit, and
// returns key+value pairs.
func (s *Store) ScanPairs(prefix []byte, limit int, reverse bool) ([]KV, error) {
	var out []KV
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: prefix, Reverse: reverse})
		defer it.Close()
		seek := prefix
		if reverse {
			high := make([]byte, len(prefix), len(prefix)+1)
			copy(high, prefix)
			seek = append(high, 0xff)
		}
		for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
			k := it.Item().Key()
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			kc := make([]byte, len(k))
			copy(kc, k)
			out = append(out, KV{Key: kc, Value: v})
			if limit > 0 && len(out) >= limit {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage: scanpairs: %w", err)
	}
	return out, nil
}

// ScanCursor iterates keys under prefix in REVERSE (newest-first) starting
// from fromKey (exclusive — the cursor entry is not re-returned), up to limit
// (0 = unlimited). Returns key+value pairs. fromKey is the reconstructed seek
// key for this prefix derived from the LOGICAL (ts, post_id) cursor; when nil,
// the scan starts from the newest entry. See design.md (logical cursor).
func (s *Store) ScanCursor(prefix, fromKey []byte, limit int) ([]KV, error) {
	var out []KV
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: prefix, Reverse: true})
		defer it.Close()
		seek := fromKey
		if seek == nil {
			high := make([]byte, len(prefix), len(prefix)+1)
			copy(high, prefix)
			seek = append(high, 0xff)
		}
		for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
			k := it.Item().Key()
			if bytes.Equal(k, fromKey) {
				continue // skip the cursor entry (already shown)
			}
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			kc := make([]byte, len(k))
			copy(kc, k)
			out = append(out, KV{Key: kc, Value: v})
			if limit > 0 && len(out) >= limit {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage: scancursor: %w", err)
	}
	return out, nil
}

// AddFollower is the conditional count-bearing transaction for the followee's
// (B) shard. It writes the follower edge and increments FollowerCount in ONE
// transaction, and only when the edge is genuinely new. A duplicate follow is
// an idempotent no-op. Conflicting concurrent transactions are retried.
// Invariant: FollowerCount always equals the number of real follower edges
// (excluding the self-edge). See design.md.
func (s *Store) AddFollower(followee, follower string) error {
	edgeKey := []byte(keys.FollowersEdgeKey(followee, follower))
	userKey := []byte(keys.UserKey(followee))
	var enc []byte
	err := s.retry(func(txn *badger.Txn) error {
		if _, err := txn.Get(edgeKey); err == nil {
			return errAlreadyFollowing
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		if err := txn.Set(edgeKey, []byte{}); err != nil {
			return err
		}
		e, err := s.adjustCount(txn, userKey, +1)
		if err != nil {
			return err
		}
		enc = e
		return nil
	})
	switch err {
	case nil:
		// The follower edge and the incremented count replicate as ONE op so
		// the replica can never see the pair split (unit-of-work contract).
		s.notify(WriteSet{Pairs: []KV{{Key: edgeKey, Value: []byte{}}, {Key: userKey, Value: enc}}})
		return nil
	case errAlreadyFollowing:
		return nil // idempotent no-op; nothing written
	default:
		return err
	}
}

// RemoveFollower is the conditional mirror: deletes the follower edge and
// decrements FollowerCount only when the edge exists. Unfollow without an
// edge is an idempotent no-op.
func (s *Store) RemoveFollower(followee, follower string) error {
	edgeKey := []byte(keys.FollowersEdgeKey(followee, follower))
	userKey := []byte(keys.UserKey(followee))
	var enc []byte
	err := s.retry(func(txn *badger.Txn) error {
		if _, err := txn.Get(edgeKey); err == badger.ErrKeyNotFound {
			return errNotFollowing
		} else if err != nil {
			return err
		}
		if err := txn.Delete(edgeKey); err != nil {
			return err
		}
		e, err := s.adjustCount(txn, userKey, -1)
		if err != nil {
			return err
		}
		enc = e
		return nil
	})
	switch err {
	case nil:
		s.notify(WriteSet{
			Pairs:   []KV{{Key: userKey, Value: enc}},
			Deletes: [][]byte{edgeKey},
		})
		return nil
	case errNotFollowing:
		return nil // idempotent no-op
	default:
		return err
	}
}

// adjustCount increments or decrements a user's FollowerCount inside an open
// transaction and returns the new encoded user bytes. Returns ErrNotFound
// when the followee has no user record.
func (s *Store) adjustCount(txn *badger.Txn, userKey []byte, delta int64) ([]byte, error) {
	item, err := txn.Get(userKey)
	if err == badger.ErrKeyNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	raw, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	var u models.User
	if err := s.codec.Decode(raw, &u); err != nil {
		return nil, err
	}
	u.FollowerCount += delta
	enc, err := s.codec.Encode(&u)
	if err != nil {
		return nil, err
	}
	if err := txn.Set(userKey, enc); err != nil {
		return nil, err
	}
	return enc, nil
}

// retry runs fn in a write transaction, retrying on ErrConflict with capped
// exponential backoff. Concurrent commits to the same user record serialize
// through this loop; each retry re-reads the fresh value.
func (s *Store) retry(fn func(txn *badger.Txn) error) error {
	backoff := initialBackoff
	for attempt := 0; attempt < maxConflictRetries; attempt++ {
		err := s.db.Update(fn)
		if err == nil {
			return nil
		}
		if err != badger.ErrConflict {
			return err
		}
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	return errors.New("storage: too many conflict retries")
}

// ErrExists is returned by CreateUser when the user already exists (no
// overwrite — re-creating a user would reset FollowerCount and flip celebrity
// status, and breaks seed idempotency).
var ErrExists = errors.New("storage: user already exists")

// CreateUser writes the user record plus both self-follow edges in one
// transaction. FollowerCount starts at 0 (the self-edge is not counted).
// Returns ErrExists if the user already exists.
func (s *Store) CreateUser(u models.User) error {
	enc, err := s.codec.Encode(&u)
	if err != nil {
		return err
	}
	userKey := []byte(keys.UserKey(u.ID))
	selfFollow := []byte(keys.FollowersEdgeKey(u.ID, u.ID))
	selfFollowing := []byte(keys.FollowingEdgeKey(u.ID, u.ID))
	err = s.retry(func(txn *badger.Txn) error {
		if _, err := txn.Get(userKey); err == nil {
			return ErrExists
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		if err := txn.Set(userKey, enc); err != nil {
			return err
		}
		if err := txn.Set(selfFollow, []byte{}); err != nil {
			return err
		}
		return txn.Set(selfFollowing, []byte{})
	})
	if err != nil {
		return err
	}
	s.notify(WriteSet{Pairs: []KV{
		{Key: userKey, Value: enc},
		{Key: selfFollow, Value: []byte{}},
		{Key: selfFollowing, Value: []byte{}},
	}})
	return nil
}
