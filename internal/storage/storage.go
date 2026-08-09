package storage

import (
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
}

// KV is a raw key/value byte pair.
type KV struct {
	Key   []byte
	Value []byte
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
	return nil
}

// Batch writes all pairs in ONE transaction (all-or-nothing). Used by the
// shard's batched write endpoint and by fan-out, where a whole shard batch
// must apply atomically or not at all.
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

// AddFollower is the conditional count-bearing transaction for the followee's
// (B) shard. It writes the follower edge and increments FollowerCount in ONE
// transaction, and only when the edge is genuinely new. A duplicate follow is
// an idempotent no-op. Conflicting concurrent transactions are retried.
// Invariant: FollowerCount always equals the number of real follower edges
// (excluding the self-edge). See design.md.
func (s *Store) AddFollower(followee, follower string) error {
	err := s.retry(func(txn *badger.Txn) error {
		edgeKey := []byte(keys.FollowersEdgeKey(followee, follower))
		if _, err := txn.Get(edgeKey); err == nil {
			return errAlreadyFollowing
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		if err := txn.Set(edgeKey, []byte{}); err != nil {
			return err
		}
		return s.adjustCount(txn, followee, +1)
	})
	switch err {
	case nil, errAlreadyFollowing:
		return nil
	default:
		return err
	}
}

// RemoveFollower is the conditional mirror: deletes the follower edge and
// decrements FollowerCount only when the edge exists. Unfollow without an
// edge is an idempotent no-op.
func (s *Store) RemoveFollower(followee, follower string) error {
	err := s.retry(func(txn *badger.Txn) error {
		edgeKey := []byte(keys.FollowersEdgeKey(followee, follower))
		if _, err := txn.Get(edgeKey); err == badger.ErrKeyNotFound {
			return errNotFollowing
		} else if err != nil {
			return err
		}
		if err := txn.Delete(edgeKey); err != nil {
			return err
		}
		return s.adjustCount(txn, followee, -1)
	})
	switch err {
	case nil, errNotFollowing:
		return nil
	default:
		return err
	}
}

// adjustCount increments or decrements a user's FollowerCount inside an open
// transaction. Returns ErrNotFound when the followee has no user record.
func (s *Store) adjustCount(txn *badger.Txn, userID string, delta int64) error {
	userKey := []byte(keys.UserKey(userID))
	item, err := txn.Get(userKey)
	if err == badger.ErrKeyNotFound {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	raw, err := item.ValueCopy(nil)
	if err != nil {
		return err
	}
	var u models.User
	if err := s.codec.Decode(raw, &u); err != nil {
		return err
	}
	u.FollowerCount += delta
	enc, err := s.codec.Encode(&u)
	if err != nil {
		return err
	}
	return txn.Set(userKey, enc)
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
	return s.retry(func(txn *badger.Txn) error {
		if _, err := txn.Get([]byte(keys.UserKey(u.ID))); err == nil {
			return ErrExists
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		if err := txn.Set([]byte(keys.UserKey(u.ID)), enc); err != nil {
			return err
		}
		if err := txn.Set([]byte(keys.FollowersEdgeKey(u.ID, u.ID)), []byte{}); err != nil {
			return err
		}
		return txn.Set([]byte(keys.FollowingEdgeKey(u.ID, u.ID)), []byte{})
	})
}
