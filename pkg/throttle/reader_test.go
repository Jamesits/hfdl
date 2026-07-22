package throttle

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPacedTransportUnlimitedPassThrough(t *testing.T) {
	payload := bytes.Repeat([]byte("u"), 1<<20)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	b := NewBucket(0, 0, 0)
	hc := &http.Client{Transport: PacedTransport(http.DefaultTransport, b)}
	start := time.Now()
	resp, err := hc.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("body mismatch: %d bytes, want %d", len(got), len(payload))
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("unlimited paced transport blocked: %v", el)
	}
}

// TestPacedTransportPacesWireBytes is the end-to-end pacing check: reading a
// fixed body through a limited bucket must take at least
// (bytes-burst)/rate. This is the regression test for the pre-transport
// implementation whose worker-level debits were observed not to pace.
func TestPacedTransportPacesWireBytes(t *testing.T) {
	const (
		rate  = int64(256 << 10) // 256 KiB/s
		burst = int64(64 << 10)  // one paced chunk
		size  = 192 << 10        // (size-burst)/rate = 500ms floor
	)
	payload := bytes.Repeat([]byte("p"), size)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	b := NewBucket(rate, burst, burst)
	hc := &http.Client{Transport: PacedTransport(http.DefaultTransport, b)}
	start := time.Now()
	resp, err := hc.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("body mismatch: %d bytes, want %d", len(got), len(payload))
	}
	el := time.Since(start)
	// Full-rate floor minus the initial burst; generous upper bound only to
	// catch a stuck bucket, not to assert precision under CI jitter.
	if floor := time.Duration(float64(size-burst)/float64(rate)*float64(time.Second)) * 9 / 10; el < floor {
		t.Fatalf("read of %d bytes at %d B/s finished in %v, want >= %v (pacing ineffective)", size, rate, el, floor)
	}
	if el > 10*time.Second {
		t.Fatalf("read took %v, bucket appears stuck", el)
	}
}

// TestPacedTransportSharedBudget verifies two concurrent responses through
// the same transport share one bucket: the combined elapsed time reflects
// the total bytes, not per-connection budgets.
func TestPacedTransportSharedBudget(t *testing.T) {
	const (
		rate  = int64(512 << 10)
		burst = int64(64 << 10)
		size  = 192 << 10
	)
	payload := bytes.Repeat([]byte("s"), size)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	b := NewBucket(rate, burst, burst)
	hc := &http.Client{Transport: PacedTransport(http.DefaultTransport, b)}
	start := time.Now()
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			resp, err := hc.Get(srv.URL)
			if err != nil {
				errs <- err
				return
			}
			_, err = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			errs <- err
		}()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	el := time.Since(start)
	// 384 KiB total minus one burst at 512 KiB/s: >= ~600ms with slack.
	if floor := time.Duration(float64(2*size-burst)/float64(rate)*float64(time.Second)) * 9 / 10; el < floor {
		t.Fatalf("two paced streams finished in %v, want >= %v (bucket not shared)", el, floor)
	}
}
