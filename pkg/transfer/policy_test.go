package transfer

import (
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
)

func policyTask(urls ...string) *FileTask {
	ups := make([]*Upstream, len(urls))
	for i, u := range urls {
		ups[i] = &Upstream{Endpoint: u}
	}
	return &FileTask{FileID: 1, Path: "p", Size: 1000, Repo: "o/r", SHA: "s", Upstreams: ups}
}

func TestPolicyRandomDistribution(t *testing.T) {
	urls := []string{"http://a", "http://b", "http://c"}
	task := policyTask(urls...)
	task.Policy = config.Random
	src := newTestHTTPSource(task, testSeed(11))
	counts := map[string]int{}
	const n = 30000
	for range n {
		u, err := src.pick(time.Now())
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		counts[u.Endpoint]++
	}
	for _, u := range urls {
		got := float64(counts[u]) / n
		if got < 0.30 || got > 0.37 {
			t.Fatalf("random share for %s = %.3f, want ~0.333", u, got)
		}
	}
}

func TestPolicyRoundRobinExact(t *testing.T) {
	urls := []string{"http://a", "http://b", "http://c"}
	task := policyTask(urls...)
	task.Policy = config.RoundRobin
	src := newTestHTTPSource(task, testSeed(12))
	for round := range 5 {
		for _, want := range urls {
			u, err := src.pick(time.Now())
			if err != nil {
				t.Fatalf("pick: %v", err)
			}
			if u.Endpoint != want {
				t.Fatalf("round %d: got %s, want %s", round, u.Endpoint, want)
			}
		}
	}
}

// TestPolicyRoundRobinSkipsUnhealthy: the atomic counter rotates over the
// healthy set only.
func TestPolicyRoundRobinSkipsUnhealthy(t *testing.T) {
	task := policyTask("http://a", "http://b", "http://c")
	task.Policy = config.RoundRobin
	src := newTestHTTPSource(task, testSeed(13))
	src.setCooldown("http://b", time.Now().Add(time.Minute))
	for i, want := range []string{"http://a", "http://c", "http://a", "http://c"} {
		u, err := src.pick(time.Now())
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		if u.Endpoint != want {
			t.Fatalf("pick %d: got %s, want %s", i, u.Endpoint, want)
		}
	}
}

// TestPolicyBestSpeedExploits: BestSpeed picks the max-EMA upstream ~90% of
// the time (ε=0.1 exploration spreads the rest uniformly).
func TestPolicyBestSpeedExploits(t *testing.T) {
	task := policyTask("http://slow", "http://fast", "http://mid")
	task.Policy = config.BestSpeed
	src := newTestHTTPSource(task, testSeed(14))
	src.ema["http://slow"] = 100
	src.ema["http://fast"] = 900
	src.ema["http://mid"] = 400
	counts := map[string]int{}
	const n = 30000
	for range n {
		u, err := src.pick(time.Now())
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		counts[u.Endpoint]++
	}
	fast := float64(counts["http://fast"]) / n
	if fast < 0.88 || fast > 0.95 {
		t.Fatalf("exploit share %.3f, want ~0.933", fast)
	}
	for _, u := range []string{"http://slow", "http://mid"} {
		got := float64(counts[u]) / n
		if got < 0.02 || got > 0.06 {
			t.Fatalf("explore share for %s %.3f, want ~0.033", u, got)
		}
	}
}

// TestPolicyEMAUpdate: report/penalize steer BestSpeed, mirroring the
// registry math (α=0.2, penalty ×0.5).
func TestPolicyEMAUpdate(t *testing.T) {
	task := policyTask("http://a", "http://b")
	task.Policy = config.BestSpeed
	src := newTestHTTPSource(task, testSeed(15))
	src.ema["http://a"] = 100
	src.ema["http://b"] = 200
	src.penalize("http://b")          // 200 → 100, tie
	src.penalize("http://b")          // → 50, a leads strictly
	src.reportRate("http://a", 10000) // ema a → 0.2*10000+0.8*100 = 2080
	// a must dominate (ε-exploration still sends ~10% elsewhere).
	wins := 0
	for range 100 {
		u, err := src.pick(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if u.Endpoint == "http://a" {
			wins++
		}
	}
	if wins < 80 {
		t.Fatalf("best-speed failed to exploit new leader: %d/100", wins)
	}
}

// TestPolicyNoHealthy: cooled-down everything is retriable FailNoHealthy.
func TestPolicyNoHealthy(t *testing.T) {
	task := policyTask("http://a")
	src := newTestHTTPSource(task, testSeed(16))
	src.setCooldown("http://a", time.Now().Add(time.Minute))
	_, err := src.pick(time.Now())
	assertKind(t, err, FailNoHealthy)
	// Expired cooldown heals.
	src.cooldown["http://a"] = time.Now().Add(-time.Second)
	if _, err := src.pick(time.Now()); err != nil {
		t.Fatalf("pick after cooldown expiry: %v", err)
	}
	// Store-seeded blacklist honored.
	task2 := policyTask("http://a")
	task2.Upstreams[0].BlacklistUntil = time.Now().Add(time.Minute)
	src2 := newTestHTTPSource(task2, testSeed(17))
	_, err = src2.pick(time.Now())
	assertKind(t, err, FailNoHealthy)
}

func TestPenalizeBlacklistsUntilTTLExpiry(t *testing.T) {
	task := policyTask("http://bad", "http://good")
	task.Policy = config.RoundRobin
	src := newTestHTTPSource(task, testSeed(18))
	src.blacklistTTL = 20 * time.Millisecond
	d := testDownloader(t)
	fd := &fileDownload{d: d, hs: src}

	fd.penalize("http://bad")
	until, ok := src.cooldown["http://bad"]
	if !ok || !until.After(time.Now()) {
		t.Fatal("penalize did not install a temporary blacklist")
	}
	for range 4 {
		got, err := src.pick(time.Now())
		if err != nil {
			t.Fatalf("pick while blacklisted: %v", err)
		}
		if got.Endpoint != "http://good" {
			t.Fatalf("picked blacklisted endpoint %q", got.Endpoint)
		}
	}
	got, err := src.pick(until.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("pick after TTL: %v", err)
	}
	if got.Endpoint != "http://bad" {
		t.Fatalf("endpoint did not return after TTL: got %q", got.Endpoint)
	}
}

func assertKind(t *testing.T, err error, kind FailKind) {
	t.Helper()
	ae, ok := err.(*AttemptError)
	if !ok || ae.Kind != kind {
		t.Fatalf("got %v (%T), want AttemptError kind %d", err, err, kind)
	}
}
