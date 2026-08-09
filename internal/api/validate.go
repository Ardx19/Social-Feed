package api

import (
	"strings"
)

const maxIDLen = 64

// validateID rejects client-supplied IDs that would break key encoding or
// prefix scans. A user named "bob" would lexicographically prefix-match
// "bob:x"'s data, so ':' and control characters are rejected. Applied to EVERY
// client-supplied ID (users, follow, feed, posts) — see design.md. It is a
// decision to centralize this here so no handler assembles a key from an
// unvalidated ID.
func validateID(id string) bool {
	if id == "" || len(id) > maxIDLen {
		return false
	}
	for _, r := range id {
		if r == ':' || r <= 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func validName(name string) bool {
	return len(strings.TrimSpace(name)) > 0 && len(name) <= 128
}
