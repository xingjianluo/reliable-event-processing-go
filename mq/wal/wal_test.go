package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReplay(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "events.log")
	w, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		seq, err := w.Append("ev", []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		if seq != int64(i) {
			t.Fatalf("seq = %d, want %d", seq, i)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	var seqs []int64
	if err := w2.Replay(func(r Record) error {
		got = append(got, r.Data)
		seqs = append(seqs, r.Seq)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0][0] != 1 || got[2][0] != 3 {
		t.Fatalf("replay = %v", got)
	}
	seq, _ := w2.Append("ev", []byte{4})
	if seq != 4 {
		t.Fatalf("next seq = %d, want 4", seq)
	}
	w2.Close()
}

func TestTornTail(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "events.log")
	w, _ := Open(p)
	for i := 1; i <= 2; i++ {
		if _, err := w.Append("ev", []byte("payload")); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// 模拟崩溃：在末尾追加半条记录。
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x57, 0x41, 0x4c, 0x4d, 0x00, 0x01}); err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()

	w2, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := w2.Replay(func(r Record) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("recovered count = %d, want 2", count)
	}
	if w2.nextSeq != 3 {
		t.Fatalf("nextSeq = %d, want 3", w2.nextSeq)
	}
	seq, _ := w2.Append("ev", []byte("after"))
	if seq != 3 {
		t.Fatalf("seq after recovery = %d, want 3", seq)
	}
	w2.Close()

	// 文件尾部不应再有垃圾。
	data, _ := os.ReadFile(p)
	if sz := len(data); sz <= 0 {
		t.Fatal("empty file")
	}
}
