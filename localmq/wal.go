package localmq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// On-disk frame: [4 bytes big-endian payload length][4 bytes CRC32 IEEE of
// payload][payload]. Frames are appended sequentially. A process (or host)
// crash can only damage the final, partially-written frame; Open detects it
// (short read or CRC mismatch) and truncates the file back to the last intact
// frame, so replay always observes complete, ordered records.

const walName = "wal.log"

type wal struct {
	mu    sync.Mutex
	f     *os.File
	path  string
	fsync bool
	end   int64 // current file length (== next write offset)
}

func openWAL(dir string, doFsync bool) (*wal, error) {
	if dir == "" {
		return nil, errors.New("localmq: empty data directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("localmq: mkdir %s: %w", dir, err)
	}
	path := filepath.Join(dir, walName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("localmq: open wal: %w", err)
	}
	w := &wal{f: f, path: path, fsync: doFsync}

	// Recover: walk frames; truncate a torn/corrupt tail.
	validEnd, err := w.recoverTail()
	if err != nil {
		f.Close()
		return nil, err
	}
	if validEnd < w.end {
		if err := f.Truncate(validEnd); err != nil {
			f.Close()
			return nil, fmt.Errorf("localmq: truncate torn tail: %w", err)
		}
		if err := f.Sync(); err != nil && doFsync {
			f.Close()
			return nil, fmt.Errorf("localmq: sync truncate: %w", err)
		}
		w.end = validEnd
	}
	if _, err := f.Seek(w.end, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("localmq: seek: %w", err)
	}
	return w, nil
}

// recoverTail scans the file and returns the byte offset just past the last
// intact frame. It also sets w.end to the actual file length.
func (w *wal) recoverTail() (int64, error) {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	var off int64
	for {
		st, err := w.f.Stat()
		if err != nil {
			return 0, err
		}
		w.end = st.Size()
		if off >= w.end {
			return off, nil
		}
		frameStart := off
		payload, err := readFrame(w.f)
		if err == io.EOF && off == 0 {
			return 0, nil
		}
		if errors.Is(err, io.EOF) && frameStart == 0 {
			return 0, nil
		}
		if err != nil {
			// UnexpectedEOF (torn write) or CRC mismatch: everything from the
			// start of this frame onwards is unusable.
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errCRCMismatch) ||
				errors.Is(err, io.EOF) {
				return frameStart, nil
			}
			return 0, fmt.Errorf("localmq: scan wal: %w", err)
		}
		off += int64(4 + 4 + len(payload))
	}
}

var errCRCMismatch = errors.New("localmq: crc mismatch")

// append writes one frame. With fsync enabled the data and its metadata are
// durable before the call returns. Returns the byte offset of the payload.
func (w *wal) append(payload []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(payload) == 0 {
		return 0, errors.New("localmq: refusing to append empty payload")
	}
	var buf bytes.Buffer
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	buf.Write(lenBuf[:])
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc32.ChecksumIEEE(payload))
	buf.Write(crcBuf[:])
	buf.Write(payload)

	off := w.end
	if _, err := w.f.Seek(off, io.SeekStart); err != nil {
		return 0, fmt.Errorf("localmq: seek append: %w", err)
	}
	if _, err := w.f.Write(buf.Bytes()); err != nil {
		return 0, fmt.Errorf("localmq: write frame: %w", err)
	}
	if w.fsync {
		if err := w.f.Sync(); err != nil {
			return 0, fmt.Errorf("localmq: fsync frame: %w", err)
		}
	}
	w.end += int64(buf.Len())
	return off, nil
}

// replay reads every intact frame from the beginning and invokes fn with the
// payload in order. It must be called before any append (or with no
// concurrent appends).
func (w *wal) replay(fn func(off int64, payload []byte) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var off int64
	for off < w.end {
		payload, err := readFrame(w.f)
		if err != nil {
			return fmt.Errorf("localmq: replay at %d: %w", off, err)
		}
		if err := fn(off, payload); err != nil {
			return err
		}
		off += int64(4 + 4 + len(payload))
	}
	if _, err := w.f.Seek(w.end, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fsync {
		if err := w.f.Sync(); err != nil {
			w.f.Close()
			return err
		}
	}
	return w.f.Close()
}

func readFrame(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err // io.EOF or io.ErrUnexpectedEOF
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > 64*1024*1024 {
		return nil, fmt.Errorf("localmq: frame length %d implausible", n)
	}
	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return nil, err
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(crcBuf[:]) != crc32.ChecksumIEEE(payload) {
		return nil, errCRCMismatch
	}
	return payload, nil
}
