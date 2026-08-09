package codec

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"
)

// Codec serializes model structs to compact bytes. Values are msgpack-encoded
// and, when large enough to benefit, zstd-compressed. A leading 1-byte flag
// records which form was used so Decode can reverse it.
//
// Compression threshold: zstd frames carry ~10+ bytes of overhead, so
// compressing tiny values (an Edge is two IDs, ~40 bytes) inflates them. Below
// the threshold the msgpack payload is stored raw with flag 0; at/above it the
// payload is zstd-compressed with flag 1. See design.md — this is the
// "measured, not assumed" compression decision.
const (
	flagRaw  = byte(0)
	flagZstd = byte(1)

	// compressionThreshold is the msgpack payload size above which zstd is
	// applied. Chosen so that small graph edges skip compression while
	// realistic post/feed content benefits from it.
	compressionThreshold = 64
)

// Codec is safe for concurrent use.
type Codec struct {
	enc *zstd.Encoder
	dec *zstd.Decoder
}

// New builds a Codec and initializes the zstd encoder/decoder.
func New() (*Codec, error) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("codec: zstd encoder: %w", err)
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("codec: zstd decoder: %w", err)
	}
	return &Codec{enc: enc, dec: dec}, nil
}

// Encode serializes v into the flag-prefixed byte form.
func (c *Codec) Encode(v any) ([]byte, error) {
	msg, err := msgpack.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("codec: msgpack encode: %w", err)
	}
	if len(msg) >= compressionThreshold {
		comp := c.enc.EncodeAll(msg, nil)
		out := make([]byte, 0, 1+len(comp))
		out = append(out, flagZstd)
		return append(out, comp...), nil
	}
	out := make([]byte, 0, 1+len(msg))
	out = append(out, flagRaw)
	return append(out, msg...), nil
}

// Decode reverses Encode into v.
func (c *Codec) Decode(data []byte, v any) error {
	if len(data) == 0 {
		return fmt.Errorf("codec: empty value")
	}
	flag, payload := data[0], data[1:]
	var msg []byte
	switch flag {
	case flagRaw:
		msg = payload
	case flagZstd:
		msg, _ = c.dec.DecodeAll(payload, nil)
	default:
		return fmt.Errorf("codec: unknown flag byte %d", flag)
	}
	if err := msgpack.Unmarshal(msg, v); err != nil {
		return fmt.Errorf("codec: msgpack decode: %w", err)
	}
	return nil
}
