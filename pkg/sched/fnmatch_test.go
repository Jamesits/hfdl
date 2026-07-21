package sched

import "testing"

// TestFnmatch pins the python-fnmatch semantics the include/exclude filter
// implements: `*` and `?` cross `/`, classes work, everything else literal.
func TestFnmatch(t *testing.T) {
	cases := []struct {
		pat, name string
		want      bool
	}{
		// `*` crosses path separators (the python behavior that makes
		// "*.bin" match "a/b/c.bin").
		{"*.bin", "c.bin", true},
		{"*.bin", "a/b/c.bin", true},
		{"*.bin", "a/b/c.binx", false},
		{"*", "a/b/c", true},
		{"a/*", "a/b/c/d.txt", true},
		{"a/*", "b/c.txt", false},
		// `?` is one char, also crossing `/`.
		{"a?c", "abc", true},
		{"a?c", "a/c", true},
		{"a?c", "ac", false},
		// Character classes.
		{"[a-c]x", "bx", true},
		{"[a-c]x", "dx", false},
		{"[!a-c]x", "dx", true},
		{"[!a-c]x", "bx", false},
		{"[]a]x", "]x", true},
		// Literals and metachar escaping.
		{"a.b", "a.b", true},
		{"a.b", "aXb", false},
		{"a[b", "a[b", true}, // unterminated class is literal
		{"a+b", "a+b", true},
		{"config.json", "config.json", true},
		{"config.json", "xconfig.json", false},
		// Full-string anchoring.
		{"ab", "abc", false},
		{"ab", "xab", false},
	}
	for _, tc := range cases {
		if got := fnmatch(tc.name, tc.pat); got != tc.want {
			t.Errorf("fnmatch(%q, %q) = %v, want %v", tc.name, tc.pat, got, tc.want)
		}
	}
}

// TestSelection exercises include/exclude precedence and explicit filenames.
func TestSelection(t *testing.T) {
	s := &selection{include: []string{"*.bin"}, exclude: []string{"drop/*"}}
	if !s.matches("a/b.bin") {
		t.Error("a/b.bin should match include *.bin")
	}
	if s.matches("drop/x.bin") {
		t.Error("drop/x.bin should be excluded")
	}
	if s.matches("a/b.txt") {
		t.Error("a/b.txt should not match include *.bin")
	}

	// Explicit filenames bypass the globs entirely.
	f := &selection{filenames: map[string]bool{"a.txt": true}, include: []string{"*.bin"}}
	if !f.matches("a.txt") {
		t.Error("explicit filename must match")
	}
	if f.matches("b.bin") {
		t.Error("include must be ignored when filenames are explicit")
	}

	// Empty selection matches everything.
	if !(&selection{}).matches("anything/at/all") {
		t.Error("empty selection must match everything")
	}
}
