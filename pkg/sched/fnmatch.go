package sched

import (
	"regexp"
	"strings"
	"sync"
)

// Python-fnmatch semantics for include/exclude globs (huggingface_hub
// parity): `*` and `?` match path separators too, `[seq]`/`[!seq]` are
// character classes, everything else is literal. Translated to RE2 with a
// full-string anchor; compiled patterns are cached (the filter runs per
// tree entry, and repos carry tens of thousands).
func fnmatchTranslate(pat string) string {
	var b strings.Builder
	b.Grow(len(pat) + 8)
	b.WriteString("(?s)\\A")
	i, n := 0, len(pat)
	for i < n {
		c := pat[i]
		switch c {
		case '*':
			b.WriteString(".*")
			i++
		case '?':
			b.WriteString(".")
			i++
		case '[':
			j := i + 1
			if j < n && pat[j] == '!' {
				j++
			}
			if j < n && pat[j] == ']' {
				j++
			}
			for j < n && pat[j] != ']' {
				j++
			}
			if j >= n {
				// Unterminated class: literal '[' (CPython fnmatch).
				b.WriteString(`\[`)
				i++
				break
			}
			stuff := pat[i+1 : j]
			if strings.HasPrefix(stuff, "!") {
				stuff = "^" + stuff[1:]
			} else if strings.HasPrefix(stuff, "^") {
				stuff = `\` + stuff
			}
			// Escape regex metachars inside the class except class syntax.
			var sb strings.Builder
			for _, r := range stuff {
				switch r {
				case '\\':
					sb.WriteString(`\\`)
				default:
					sb.WriteRune(r)
				}
			}
			b.WriteString("[" + sb.String() + "]")
			i = j + 1
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
			i++
		}
	}
	b.WriteString(`\z`)
	return b.String()
}

var fnmatchCache sync.Map // pattern string -> *regexp.Regexp

// fnmatch reports whether name matches the python-fnmatch pattern.
func fnmatch(name, pat string) bool {
	re, ok := fnmatchCache.Load(pat)
	if !ok {
		compiled, err := regexp.Compile(fnmatchTranslate(pat))
		if err != nil {
			return false // unreachable: translation emits valid RE2 by construction
		}
		re, _ = fnmatchCache.LoadOrStore(pat, compiled)
	}
	return re.(*regexp.Regexp).MatchString(name)
}

// selection is one job's file filter: explicit filenames win over globs
// (hf parity: positional files and --include/--exclude are not combined).
type selection struct {
	filenames map[string]bool
	include   []string
	exclude   []string
}

func (s *selection) matches(path string) bool {
	if len(s.filenames) > 0 {
		return s.filenames[path]
	}
	if len(s.include) > 0 {
		ok := false
		for _, pat := range s.include {
			if fnmatch(path, pat) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	for _, pat := range s.exclude {
		if fnmatch(path, pat) {
			return false
		}
	}
	return true
}

// union matches when any of the given selections matches (a repo's tree
// listing is shared by all its jobs; per-job filtering is applied again at
// install time).
func unionMatches(sels []*selection, path string) bool {
	if len(sels) == 0 {
		return true
	}
	for _, s := range sels {
		if s.matches(path) {
			return true
		}
	}
	return false
}
