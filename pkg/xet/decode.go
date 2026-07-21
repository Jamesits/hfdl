package xet

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

// Chunk decompression. Verified against xet-core
// xet_core_structures/src/xorb_object/compression_scheme.rs:
//
//   - LZ4 (1): standard LZ4 *frame* format (Rust side uses lz4_flex
//     FrameEncoder/FrameDecoder; pierrec/lz4/v4 implements the same frame
//     spec, magic 0x184D2204).
//   - BG4+LZ4 (2): byte-group-4 transpose of the raw chunk, then LZ4 frame.
//     Transpose (byte_grouping/bg4.rs bg4_split_together): with
//     split=n/4, rem=n%4, group sizes are
//     split+min(rem,1), split+min(max(rem-1,0),1), split+min(max(rem-2,0),1),
//     split; group k holds bytes at positions 4i+k.
//
// decodeChunk also enforces the header's uncompressed_length: a
// decompressor that yields a different length means corrupt data, and
// downstream term validation (unpacked_length) would otherwise see a
// shifted byte stream.

func decodeChunk(xorb string, h chunkHeader, payload []byte) ([]byte, error) {
	var out []byte
	switch h.scheme {
	case schemeNone:
		out = payload
	case schemeLZ4, schemeBG4LZ4:
		// Cap the decompressor at the declared size + 1 so a malicious or
		// corrupt payload cannot expand without bound (zip-bomb guard).
		r := lz4.NewReader(bytes.NewReader(payload))
		buf, err := io.ReadAll(io.LimitReader(r, int64(h.unpackedLen)+1))
		if err != nil {
			return nil, &DataError{Xorb: xorb, Reason: "lz4 frame decode", Err: err}
		}
		out = buf
		if h.scheme == schemeBG4LZ4 {
			out = bg4Regroup(out)
		}
	default:
		return nil, &DataError{Xorb: xorb, Reason: fmt.Sprintf("unknown compression scheme id %d", h.scheme)}
	}
	if len(out) != h.unpackedLen {
		return nil, &DataError{
			Xorb:   xorb,
			Reason: fmt.Sprintf("decoded chunk length %d != header uncompressed_length %d", len(out), h.unpackedLen),
		}
	}
	return out, nil
}

// bg4Regroup inverts the BG4 transpose. Direct port of
// byte_grouping/bg4.rs bg4_regroup_together.
func bg4Regroup(g []byte) []byte {
	n := len(g)
	split := n / 4
	rem := n % 4

	s0 := split + min(rem, 1)
	s1 := split + min(max(rem-1, 0), 1)
	s2 := split + min(max(rem-2, 0), 1)

	g0 := g[:s0]
	g1 := g[s0 : s0+s1]
	g2 := g[s0+s1 : s0+s1+s2]
	g3 := g[s0+s1+s2:]

	out := make([]byte, n)
	for i := 0; i < split; i++ {
		out[4*i] = g0[i]
		out[4*i+1] = g1[i]
		out[4*i+2] = g2[i]
		out[4*i+3] = g3[i]
	}
	switch rem {
	case 1:
		out[4*split] = g0[split]
	case 2:
		out[4*split] = g0[split]
		out[4*split+1] = g1[split]
	case 3:
		out[4*split] = g0[split]
		out[4*split+1] = g1[split]
		out[4*split+2] = g2[split]
	}
	return out
}
