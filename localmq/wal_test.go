package localmq

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALTruncatesTornTail(t *testing.T) {
	dir := tempDir(t)
	w, err := openWAL(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	off1, _ := w.append([]byte("first-record"))
	off2, _ := w.append([]byte("second-record"))
	_ = off1
	_ = off2
	if err := w.close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, walName)
	good, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Append a torn frame: valid header length but truncated payload.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	torn := []byte{0, 0, 0, 9, 0, 0, 0, 0, 'x'} // claims 9 bytes, gives 1
	if _, err := f.Write(torn); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	w2, err := openWAL(dir, true) // should truncate torn tail
	if err != nil {
		t.Fatalf("reopen after torn write: %v", err)
	}
	var n int
	err = w2.replay(func(off int64, p []byte) error {
		n++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("replayed %d records want 2", n)
	}
	if err := w2.close(); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(path)
	if st.Size() != good.Size() {
		t.Fatalf("size after recover = %d want %d", st.Size(), good.Size())
	}
}

func TestWALOrderAndPayload(t *testing.T) {
	dir := tempDir(t)
	w, _ := openWAL(dir, true)
	payloads := []string{"alpha", "beta", "gamma"}
	for _, p := range payloads {
		if _, err := w.append([]byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	_ = w.replay(func(off int64, p []byte) error {
		got = append(got, string(p))
		return nil
	})
	_ = w.close()
	if len(got) != 3 || got[0] != "alpha" || got[2] != "gamma" {
		t.Fatalf("replay = %v", got)
	}
}
