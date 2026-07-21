package xet

import (
	"fmt"
)

// Xorb chunk framing, verified against xet-core
// xet_core_structures/src/xorb_object/xorb_chunk_format.rs:
//
//	XorbChunkHeader = 8 bytes, serialized field-by-field:
//	  [0]   version u8            (must be <= 0 today)
//	  [1:4] compressed_length   u24 little-endian
//	  [4]   compression_scheme  u8  (0=none, 1=LZ4, 2=BG4+LZ4)
//	  [5:8] uncompressed_length u24 little-endian
//
// followed by compressed_length payload bytes. Signed xorb ranges always
// begin at a chunk header — CAS maps chunk ranges onto serialized-xorb byte
// ranges, so the fetched bytes never include the xorb object header/footer.

const (
	chunkHeaderLen     = 8
	maxChunkUnpacked   = 128 << 10 // xet-core MAX_CHUNK_SIZE = 64KiB target * 2 multiplier (constants.rs)
	maxChunkPacked     = 2 * maxChunkUnpacked
	chunkHeaderVersion = 0
)

// Compression scheme ids (xorb_object/compression_scheme.rs).
const (
	schemeNone   = 0
	schemeLZ4    = 1
	schemeBG4LZ4 = 2
)

type chunkHeader struct {
	scheme      int
	packedLen   int
	unpackedLen int
}

// parseChunkHeader parses and validates an 8-byte header from b
// (len(b) >= chunkHeaderLen). Validation mirrors XorbChunkHeader::validate.
func parseChunkHeader(xorb string, b []byte) (chunkHeader, error) {
	var h chunkHeader
	if len(b) < chunkHeaderLen {
		return h, &DataError{Xorb: xorb, Reason: "truncated chunk header"}
	}
	if v := b[0]; v > chunkHeaderVersion {
		return h, &DataError{Xorb: xorb, Reason: fmt.Sprintf("chunk header version %d too new", v)}
	}
	h.packedLen = int(b[1]) | int(b[2])<<8 | int(b[3])<<16
	h.unpackedLen = int(b[5]) | int(b[6])<<8 | int(b[7])<<16
	switch b[4] {
	case schemeNone, schemeLZ4, schemeBG4LZ4:
		h.scheme = int(b[4])
	default:
		return h, &DataError{Xorb: xorb, Reason: fmt.Sprintf("unknown compression scheme id %d", b[4])}
	}
	if h.packedLen > maxChunkPacked {
		return h, &DataError{Xorb: xorb, Reason: fmt.Sprintf("compressed chunk length %d exceeds max %d", h.packedLen, maxChunkPacked)}
	}
	if h.unpackedLen > maxChunkUnpacked {
		return h, &DataError{Xorb: xorb, Reason: fmt.Sprintf("uncompressed chunk length %d exceeds max %d", h.unpackedLen, maxChunkUnpacked)}
	}
	return h, nil
}

// decodeXorbChunks parses data as a sequence of framed chunks and returns
// each chunk's decoded bytes. data must hold whole chunks (signed ranges
// always cover whole chunk ranges), so trailing bytes are an error.
func decodeXorbChunks(xorb string, data []byte) ([][]byte, error) {
	var chunks [][]byte
	for off := 0; off < len(data); {
		h, err := parseChunkHeader(xorb, data[off:])
		if err != nil {
			return nil, err
		}
		off += chunkHeaderLen
		if len(data)-off < h.packedLen {
			return nil, &DataError{Xorb: xorb, Reason: fmt.Sprintf("truncated chunk payload: want %d, have %d", h.packedLen, len(data)-off)}
		}
		payload := data[off : off+h.packedLen]
		off += h.packedLen
		dec, err := decodeChunk(xorb, h, payload)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, dec)
	}
	return chunks, nil
}
