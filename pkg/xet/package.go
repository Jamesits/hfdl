// Package xet implements the download side of the hf_xet protocol:
// CAS reconstruction, signed xorb range fetching, chunk decode
// (none / LZ4-frame / BG4+LZ4), and an on-disk chunk cache. It exposes
// xet-backed files to pkg/transfer as a transfer.BlockSource.
//
// Wire formats are verified against the xet-core Rust implementation
// (github.com/huggingface/xet-core); source paths are cited at each
// definition:
//
//   - Reconstruction: GET {casUrl}/v2/reconstructions/{file_id}, falling
//     back to /v1/... on 404/501 (xet_client/src/cas_client/remote_client.rs,
//     get_reconstruction_with_version_override). Both responses are plain
//     JSON (xet_client/src/cas_types/mod.rs).
//   - Xorb chunk framing: 8-byte headers
//     (xet_core_structures/src/xorb_object/xorb_chunk_format.rs).
//   - Compression ids 0=none, 1=LZ4, 2=BG4+LZ4; LZ4 is the standard LZ4
//     *frame* format (xet_core_structures/src/xorb_object/compression_scheme.rs,
//     byte_grouping/bg4.rs).
//   - Signed xorb ranges are fetched with a plain HTTP client (no CAS
//     bearer) and the Range header formed EXACTLY from the authorized byte
//     range; deviating ranges are rejected by the signer with 403
//     (xet_client/src/cas_client/remote_client.rs, get_file_term_data).
//
// All xet-domain identities (chunk/xorb/file ids) are keyed-BLAKE3 hex and
// are NEVER used as verification targets; final verification always uses
// the LFS sha256 BlobID owned by pkg/verify.
//
// CacheDir defaulting: the contract's "" = "<hf cache>/xet" default is
// resolved by cmd (only it knows the HF cache root); this package treats an
// empty CacheDir as "chunk cache disabled" rather than guessing a path.
package xet
