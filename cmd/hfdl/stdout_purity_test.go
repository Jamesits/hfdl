package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const stdoutPurityChildEnv = "HFDL_STDOUT_PURITY_CHILD"

func TestStdoutPurity(t *testing.T) {
	if os.Getenv(stdoutPurityChildEnv) == "1" {
		var args []string
		if err := json.Unmarshal([]byte(os.Getenv("HFDL_STDOUT_PURITY_ARGS")), &args); err != nil {
			fmt.Fprintln(os.Stderr, "decode child arguments:", err)
			os.Exit(1)
		}
		cmd := newRootCmd()
		cmd.SetArgs(args)
		if err := cmd.ExecuteContext(t.Context()); err != nil {
			fmt.Fprintln(os.Stderr, "Error:", err)
			os.Exit(1)
		}
		// Bypass the test harness's PASS line: stdout must contain only the
		// bytes emitted by the real Cobra command.
		os.Exit(0)
	}

	content := []byte("stdout purity fixture\n")
	hash := sha1.New()
	_, _ = fmt.Fprintf(hash, "blob %d\x00", len(content))
	_, _ = hash.Write(content)
	gitOID := hex.EncodeToString(hash.Sum(nil))
	commit := "0123456789abcdef0123456789abcdef01234567"
	const filename = "tiny.bin"

	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/models/org/repo/revision/main":
			_ = json.NewEncoder(w).Encode(map[string]string{"sha": commit})
		case r.Method == http.MethodGet && r.URL.Path == "/api/models/org/repo/tree/"+commit:
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"type": "file", "path": filename, "size": len(content),
				"oid": gitOID,
			}})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/org/repo/resolve/") && strings.HasSuffix(r.URL.Path, "/"+filename):
			w.Header().Set("ETag", `"`+gitOID+`"`)
			http.ServeContent(w, r, filename, time.Unix(0, 0), bytes.NewReader(content))
		default:
			http.Error(w, "unexpected fake Hub request: "+r.Method+" "+r.URL.RequestURI(), http.StatusNotFound)
		}
	}))
	defer hub.Close()

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	outputDir := filepath.Join(t.TempDir(), "output")
	var baseline string
	for _, quiet := range []bool{false, true} {
		name := "default"
		if quiet {
			name = "quiet"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			args := []string{
				"download", "org/repo", filename,
				"--local-dir", outputDir,
				"--hfdl-endpoint", hub.URL,
				"--hfdl-state-db", filepath.Join(root, "state", "state.db"),
				"--hfdl-no-tui",
			}
			if quiet {
				args = append(args, "--quiet")
			}
			encodedArgs, err := json.Marshal(args)
			if err != nil {
				t.Fatal(err)
			}

			child := exec.Command(exe, "-test.run=^TestStdoutPurity$")
			child.Env = append(os.Environ(),
				stdoutPurityChildEnv+"=1",
				"HFDL_STDOUT_PURITY_ARGS="+string(encodedArgs),
				"HF_HUB_CACHE="+filepath.Join(root, "cache"),
			)
			var stdout, stderr bytes.Buffer
			child.Stdout = &stdout
			child.Stderr = &stderr
			if err := child.Run(); err != nil {
				t.Fatalf("child failed: %v\nstderr:\n%s\nstdout: %q", err, stderr.String(), stdout.String())
			}

			want := filepath.Join(outputDir, filename) + "\n"
			if stdout.String() != want {
				t.Fatalf("stdout mismatch:\n got %q\nwant %q\nstderr:\n%s", stdout.String(), want, stderr.String())
			}
			if baseline == "" {
				baseline = stdout.String()
			} else if stdout.String() != baseline {
				t.Fatalf("quiet stdout %q differs from default %q", stdout.String(), baseline)
			}
		})
	}
}
