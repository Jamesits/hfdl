package netcfg

import (
	"fmt"
	"strconv"
	"strings"
)

// DefaultIPQoS mirrors OpenSSH's default values (af21, cs1):
// latency-sensitive control traffic AF21, bulk payload CS1 (lower effort).
const DefaultIPQoS = "af21,cs1"

// defaultTelemetryTOS is the telemetry class used when the spec omits the
// third value: CS1, telemetry is background traffic.
const defaultTelemetryTOS = 0x20

// TOSNone means "leave the socket untagged" (the IPQoS keyword "none").
const TOSNone = -1

// IPQoS is a parsed --hfdl-ipqos spec: one TOS/Traffic Class byte per
// traffic class, or TOSNone. API covers Hub metadata calls, Download covers
// payload transfers (CDN/CAS), Telemetry covers OTLP exports.
type IPQoS struct {
	API       int
	Download  int
	Telemetry int

	spec string
}

// String returns the normalized spec, for echoing in logs and job records.
// The zero IPQoS renders as DefaultIPQoS (with which it agrees field-wise
// only after ParseIPQoS; use ParseIPQoS to construct).
func (q IPQoS) String() string {
	if q.spec == "" {
		return DefaultIPQoS
	}
	return q.spec
}

// ipqosKeywords is OpenSSH's IPQoS keyword table (openssh misc.c): the value
// is the whole TOS byte, DSCP in the upper six bits. "le" and "reliability"
// collide at 0x04 by that same table.
var ipqosKeywords = map[string]int{
	"none":        TOSNone,
	"af11":        0x28,
	"af12":        0x30,
	"af13":        0x38,
	"af21":        0x48,
	"af22":        0x50,
	"af23":        0x58,
	"af31":        0x68,
	"af32":        0x70,
	"af33":        0x78,
	"af41":        0x88,
	"af42":        0x90,
	"af43":        0x98,
	"cs0":         0x00,
	"cs1":         0x20,
	"cs2":         0x40,
	"cs3":         0x60,
	"cs4":         0x80,
	"cs5":         0xa0,
	"cs6":         0xc0,
	"cs7":         0xe0,
	"ef":          0xb8,
	"le":          0x04,
	"lowdelay":    0x10,
	"throughput":  0x08,
	"reliability": 0x04,
}

// ParseIPQoS validates a --hfdl-ipqos spec: one to three comma-separated
// OpenSSH IPQoS values (keyword, or a 0x-prefixed hex TOS byte), in the
// order api, download, telemetry.
//
//   - one value: api and download; telemetry keeps its default (cs1)
//   - two values: api, download; telemetry keeps its default (cs1)
//   - three values: api, download, telemetry explicitly
//
// The empty spec means DefaultIPQoS, so the zero flag value is the default.
func ParseIPQoS(spec string) (IPQoS, error) {
	s := strings.TrimSpace(spec)
	if s == "" {
		s = DefaultIPQoS
	}
	parts := strings.Split(s, ",")
	if len(parts) > 3 {
		return IPQoS{}, fmt.Errorf("invalid IPQoS %q: want 1-3 comma-separated values (api[,download[,telemetry]])", spec)
	}
	tokens := make([]string, len(parts))
	vals := make([]int, len(parts))
	for i, part := range parts {
		tok := strings.TrimSpace(part)
		if tok == "" {
			return IPQoS{}, fmt.Errorf("invalid IPQoS %q: empty value", spec)
		}
		v, err := parseIPQoSToken(tok)
		if err != nil {
			return IPQoS{}, err
		}
		tokens[i], vals[i] = tok, v
	}
	q := IPQoS{spec: strings.Join(tokens, ",")}
	switch len(vals) {
	case 1:
		q.API, q.Download, q.Telemetry = vals[0], vals[0], defaultTelemetryTOS
	case 2:
		q.API, q.Download, q.Telemetry = vals[0], vals[1], defaultTelemetryTOS
	case 3:
		q.API, q.Download, q.Telemetry = vals[0], vals[1], vals[2]
	}
	return q, nil
}

// hexPrefix is required on numeric IPQoS values; bare decimal/octal is
// rejected to keep keyword typos from silently parsing as numbers.
const hexPrefix = "0x"

func parseIPQoSToken(tok string) (int, error) {
	if v, ok := ipqosKeywords[strings.ToLower(tok)]; ok {
		return v, nil
	}
	lower := strings.ToLower(tok)
	if strings.HasPrefix(lower, hexPrefix) {
		n, err := strconv.ParseUint(lower[len(hexPrefix):], 16, 16)
		if err == nil && n <= 0xff {
			return int(n), nil
		}
	}
	return 0, fmt.Errorf("invalid IPQoS value %q: want an OpenSSH keyword (af21, cs1, ef, le, none, …) or a hex TOS byte (0x00-0xff)", tok)
}
