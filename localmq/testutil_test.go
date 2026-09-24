package localmq

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	// Tests print message id / route / attempt / state transitions as
	// required; level DEBUG-ish Info goes to stdout interleaved with t.Log.
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(h)
}

func testConfig(t testing.TB, dir string) Config {
	t.Helper()
	c := DefaultConfig(dir)
	c.Logger = testLogger()
	c.RetryBaseDelay = 5 * time.Millisecond
	c.RetryMaxDelay = 50 * time.Millisecond
	c.ScheduleTick = 2 * time.Millisecond
	c.CloseTimeout = 10 * time.Second
	return c
}

func tempDir(t testing.TB) string {
	t.Helper()
	d, err := os.MkdirTemp("", "localmq-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}

func waitFor(t testing.TB, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for: %s", msg)
	}
}
