package xet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"

	"github.com/jamesits/hfdl/pkg/logging"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// Wire shapes below mirror xet_client/src/cas_types/mod.rs exactly. serde
// defaults apply: snake_case keys, hashes as hex strings (HexMerkleHash),
// chunk ranges half-open {start,end}, HTTP byte ranges inclusive-end.
// V2 is a compact JSON envelope (no orc/arrow encoding on the wire).

type chunkRangeJSON struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
}

type termJSON struct {
	Hash           string         `json:"hash"`
	UnpackedLength uint32         `json:"unpacked_length"`
	Range          chunkRangeJSON `json:"range"`
}

type byteRangeJSON struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"` // inclusive (HttpRange, cas_types/mod.rs)
}

// V1: QueryReconstructionResponse { offset_into_first_range, terms, fetch_info }.
type fetchInfoJSON struct {
	Range    chunkRangeJSON `json:"range"`
	URL      string         `json:"url"`
	URLRange byteRangeJSON  `json:"url_range"`
}

type reconstructionV1JSON struct {
	OffsetIntoFirstRange int64                      `json:"offset_into_first_range"`
	Terms                []termJSON                 `json:"terms"`
	FetchInfo            map[string][]fetchInfoJSON `json:"fetch_info"`
}

// V2: QueryReconstructionResponseV2 { offset_into_first_range, terms, xorbs }.
type rangeDescriptorJSON struct {
	Chunks chunkRangeJSON `json:"chunks"`
	Bytes  byteRangeJSON  `json:"bytes"`
}

type multiRangeFetchJSON struct {
	URL    string                `json:"url"`
	Ranges []rangeDescriptorJSON `json:"ranges"`
}

type reconstructionV2JSON struct {
	OffsetIntoFirstRange int64                            `json:"offset_into_first_range"`
	Terms                []termJSON                       `json:"terms"`
	Xorbs                map[string][]multiRangeFetchJSON `json:"xorbs"`
}

// fetchRange is one authorized (xorb, chunk-range, byte-range, URL) tuple.
// The byte range MUST be requested verbatim: deviating from the signed
// range is answered 403. V1 (range,url,url_range) and V2
// (url,ranges[{chunks,bytes}]) both normalize to this; hf_xet's default
// (enable_multirange_fetching=false) issues one single-range GET per such
// descriptor (file_reconstruction/.../file_term.rs retrieve_file_term_block).
type fetchRange struct {
	url                  string
	chunkStart, chunkEnd uint32
	byteStart, byteEnd   int64 // inclusive end
}

// reconTerm is a reconstruction term with its decoded file byte range
// resolved (terms arrive in file order; offsets accumulate by
// unpacked_length).
type reconTerm struct {
	xorb                 string
	unpackedLength       int64
	chunkStart, chunkEnd uint32
	fileStart, fileEnd   int64 // decoded file bytes, exclusive end
}

// reconstruction is the normalized (version-agnostic) form of both
// responses. baseFileOffset is 0 for full-file reconstructions; for partial
// (Range) queries the first term's decoded output starts
// offsetIntoFirstRange bytes before baseFileOffset's logical position
// (cas_types/mod.rs: "the location of [range start] into the first range").
type reconstruction struct {
	version              int // 1 or 2
	baseFileOffset       int64
	offsetIntoFirstRange int64
	terms                []reconTerm
	fetch                map[string][]fetchRange // xorb hash -> entries sorted by chunkStart
}

// normalize builds the internal form. Terms are kept in wire order (the CAS
// emits them in file order) and mapped to cumulative decoded file offsets.
// The first term's decoded bytes start offsetFirst bytes BEFORE base (for
// partial queries the first term covers bytes preceding the requested
// range), so decoded offsets address true file positions.
func normalize(version int, base int64, offsetFirst int64, terms []termJSON, fetch map[string][]fetchRange) *reconstruction {
	r := &reconstruction{
		version:              version,
		baseFileOffset:       base,
		offsetIntoFirstRange: offsetFirst,
		fetch:                fetch,
	}
	cur := base - offsetFirst
	for _, t := range terms {
		rt := reconTerm{
			xorb:           t.Hash,
			unpackedLength: int64(t.UnpackedLength),
			chunkStart:     t.Range.Start,
			chunkEnd:       t.Range.End,
			fileStart:      cur,
			fileEnd:        cur + int64(t.UnpackedLength),
		}
		cur = rt.fileEnd
		r.terms = append(r.terms, rt)
	}
	for _, entries := range fetch {
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].chunkStart != entries[j].chunkStart {
				return entries[i].chunkStart < entries[j].chunkStart
			}
			return entries[i].chunkEnd < entries[j].chunkEnd
		})
	}
	return r
}

func normalizeV1(v *reconstructionV1JSON, base int64) *reconstruction {
	fetch := make(map[string][]fetchRange, len(v.FetchInfo))
	for hash, infos := range v.FetchInfo {
		for _, fi := range infos {
			fetch[hash] = append(fetch[hash], fetchRange{
				url:        fi.URL,
				chunkStart: fi.Range.Start,
				chunkEnd:   fi.Range.End,
				byteStart:  fi.URLRange.Start,
				byteEnd:    fi.URLRange.End,
			})
		}
	}
	return normalize(1, base, v.OffsetIntoFirstRange, v.Terms, fetch)
}

func normalizeV2(v *reconstructionV2JSON, base int64) *reconstruction {
	fetch := make(map[string][]fetchRange, len(v.Xorbs))
	for hash, fetches := range v.Xorbs {
		for _, f := range fetches {
			for _, d := range f.Ranges {
				fetch[hash] = append(fetch[hash], fetchRange{
					url:        f.URL,
					chunkStart: d.Chunks.Start,
					chunkEnd:   d.Chunks.End,
					byteStart:  d.Bytes.Start,
					byteEnd:    d.Bytes.End,
				})
			}
		}
	}
	return normalize(2, base, v.OffsetIntoFirstRange, v.Terms, fetch)
}

// maxErrorBody caps error-response reads.
const maxErrorBody = 4 << 10

// fetchReconstruction performs GET {cas}/v2/reconstructions/{fileID},
// falling back to /v1/... on 404/501 (remote_client.rs
// get_reconstruction_with_version_override). When rng is non-nil a
// `Range: bytes=start-end` (inclusive end) header scopes the query and the
// response's offset_into_first_range applies for partial files.
func (c *Client) fetchReconstruction(ctx context.Context, route, fileID string, rng *byteRangeJSON) (*reconstruction, error) {
	tok, err := c.casToken(ctx, route)
	if err != nil {
		return nil, err
	}
	base := c.casBase(tok)

	ctx, sp := c.tracer.Start(ctx, "xet.reconstruct")
	defer sp.End()

	rangeHeader := ""
	// baseForTerms: file offset of the first decoded term byte. For partial
	// queries the first term is clipped by offset_into_first_range, resolved
	// after the response arrives (see normalize caller below).
	var baseForTerms int64
	if rng != nil {
		rangeHeader = fmt.Sprintf("bytes=%d-%d", rng.Start, rng.End)
		baseForTerms = rng.Start
	}

	url2 := base + "/v2/reconstructions/" + fileID
	recon, fallback, err := c.getReconV2(ctx, route, url2, rangeHeader, baseForTerms)
	if err != nil {
		sp.RecordError(err)
		sp.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	if recon == nil {
		url1 := base + "/v1/reconstructions/" + fileID
		c.log.Info("v2 reconstruction unavailable, falling back to v1", "file", fileID, "reason", fallback)
		recon, err = c.getReconV1(ctx, route, url1, rangeHeader, baseForTerms)
		if err != nil {
			sp.RecordError(err)
			sp.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		sp.SetAttributes(attribute.String("hfdl.xet.fallback", fallback))
	}
	sp.SetAttributes(
		attribute.Int("hfdl.xet.version", recon.version),
		attribute.Int("hfdl.xet.terms", len(recon.terms)),
	)
	c.log.Debug("reconstruction fetched",
		"file", fileID, "version", recon.version, "terms", len(recon.terms),
		"ranges", logging.JSONValue(termRanges(recon)))
	return recon, nil
}

func termRanges(r *reconstruction) [][2]int64 {
	out := make([][2]int64, len(r.terms))
	for i, t := range r.terms {
		out[i] = [2]int64{t.fileStart, t.fileEnd}
	}
	return out
}

// statusError drains a non-2xx CAS reconstruction response into a typed or
// wrapped error; 416 maps to ErrRangeNotSatisfiable (hf_xet's Ok(None)).
func statusError(resp *http.Response, what string) error {
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return ErrRangeNotSatisfiable
	}
	return fmt.Errorf("xet: %s: unexpected status %s: %s", what, resp.Status, string(b))
}

func (c *Client) getReconV2(ctx context.Context, route, reqURL, rangeHeader string, base int64) (recon *reconstruction, fallbackReason string, err error) {
	resp, err := c.doCAS(ctx, route, reqURL, rangeHeader)
	if err != nil {
		return nil, "", err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		defer resp.Body.Close()
		var v reconstructionV2JSON
		if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
			return nil, "", fmt.Errorf("xet: decode v2 reconstruction: %w", err)
		}
		return normalizeV2(&v, base), "", nil
	case http.StatusNotFound:
		_ = resp.Body.Close()
		return nil, "404", nil
	case http.StatusNotImplemented:
		_ = resp.Body.Close()
		return nil, "501", nil
	default:
		return nil, "", statusError(resp, "v2 reconstruction")
	}
}

func (c *Client) getReconV1(ctx context.Context, route, reqURL, rangeHeader string, base int64) (*reconstruction, error) {
	resp, err := c.doCAS(ctx, route, reqURL, rangeHeader)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, statusError(resp, "v1 reconstruction")
	}
	defer resp.Body.Close()
	var v reconstructionV1JSON
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return nil, fmt.Errorf("xet: decode v1 reconstruction: %w", err)
	}
	return normalizeV1(&v, base), nil
}
