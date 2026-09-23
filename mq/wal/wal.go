// Package wal 提供基于单文件追加 + fsync 的简单持久化日志，
// 供可靠消息系统记录消息事件（发布/确认/拒绝/死信等）。
//
// 帧格式（大端）：
//
//	+---------+---------+-----------+-----------+--------+--------+----------+
//	| magic 4 | seq  8  | kindLen 2 | dataLen 4 | kind N | data M | crc32  4 |
//	+---------+---------+-----------+-----------+--------+--------+----------+
//
// crc32(IEEE) 覆盖 seq..data 的全部字节。重放时若遇到尾帧撕裂
// （短读或 CRC 不符），会截断到最后一条完整记录的边界后再继续。
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const magic uint32 = 0x57414C4D // "WALM"

// Record 是 WAL 中的一条记录。
type Record struct {
	Seq  int64  // 单调递增序号（从 1 开始）
	Kind string // 记录类型（业务自定义）
	Data []byte // 负载
}

// WAL 是追加式预写日志。
type WAL struct {
	mu      sync.Mutex
	f       *os.File
	path    string
	nextSeq int64
	locked  bool
}

// Open 打开（不存在则创建）位于 path 的日志文件，并重放以确定末位序号；
// 同时对文件加排他锁，禁止同一目录被两个进程同时打开。
func Open(path string) (*WAL, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("wal: mkdir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: acquire lock %s: %w", path, err)
	}

	w := &WAL{f: f, path: path, nextSeq: 1, locked: true}
	lastGood, lastSeq, err := w.scan(nil)
	if err != nil {
		f.Close()
		return nil, err
	}
	// 截断异常退出可能残留的撕裂尾帧。
	if fi, statErr := f.Stat(); statErr == nil && fi.Size() != lastGood {
		if err := f.Truncate(lastGood); err != nil {
			f.Close()
			return nil, fmt.Errorf("wal: truncate torn tail: %w", err)
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: seek end: %w", err)
	}
	w.nextSeq = lastSeq + 1
	return w, nil
}

// Append 追加一条记录并 fsync 持久化，返回分配的序号。
func (w *WAL) Append(kind string, data []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	kindB := []byte(kind)
	if len(kindB) > 65535 {
		return 0, errors.New("wal: kind too long")
	}
	seq := w.nextSeq
	payloadLen := 8 + 2 + 4 + len(kindB) + len(data)
	buf := make([]byte, 4+payloadLen+4)
	binary.BigEndian.PutUint32(buf[0:4], magic)
	binary.BigEndian.PutUint64(buf[4:12], uint64(seq))
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(kindB)))
	binary.BigEndian.PutUint32(buf[14:18], uint32(len(data)))
	copy(buf[18:], kindB)
	copy(buf[18+len(kindB):], data)
	csum := crc32.ChecksumIEEE(buf[4 : 4+payloadLen])
	binary.BigEndian.PutUint32(buf[4+payloadLen:], csum)

	if _, err := w.f.Write(buf); err != nil {
		return 0, fmt.Errorf("wal: write: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return 0, fmt.Errorf("wal: sync: %w", err)
	}
	w.nextSeq++
	return seq, nil
}

// Replay 从头读取所有完整记录，对每条调用 fn。撕裂尾帧被忽略。
func (w *WAL) Replay(fn func(Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek start: %w", err)
	}
	_, _, err := w.scan(fn)
	if err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("wal: seek end: %w", err)
	}
	return nil
}

// scan 顺序扫描文件，返回最后一条完整记录后的偏移、其序号；
// 遇到 EOF（正常结尾）停止，遇到短读/CRC 错误视为尾帧撕裂并停止。
func (w *WAL) scan(emit func(Record) error) (int64, int64, error) {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("wal: seek: %w", err)
	}
	r := w.f
	var off int64
	var lastSeq int64

	var header [4 + 8 + 2 + 4]byte
	for {
		n, err := io.ReadFull(r, header[:])
		_ = n
		if err == io.EOF {
			return off, lastSeq, nil // 正好在记录边界结束
		}
		if err == io.ErrUnexpectedEOF {
			return off, lastSeq, nil // 撕裂尾帧
		}
		if err != nil {
			return 0, 0, fmt.Errorf("wal: read header: %w", err)
		}
		if binary.BigEndian.Uint32(header[0:4]) != magic {
			return 0, 0, fmt.Errorf("wal: bad magic at offset %d", off)
		}
		seq := int64(binary.BigEndian.Uint64(header[4:12]))
		kindLen := int(binary.BigEndian.Uint16(header[12:14]))
		dataLen := int(binary.BigEndian.Uint32(header[14:18]))

		body := make([]byte, kindLen+dataLen+4)
		if _, err := io.ReadFull(r, body); err != nil {
			return off, lastSeq, nil // 尾帧不完整
		}
		storedCrc := binary.BigEndian.Uint32(body[len(body)-4:])
		crcBuf := make([]byte, 0, 14+kindLen+dataLen)
		crcBuf = append(crcBuf, header[4:]...)
		crcBuf = append(crcBuf, body[:kindLen+dataLen]...)
		if crc32.ChecksumIEEE(crcBuf) != storedCrc {
			return off, lastSeq, nil // CRC 不符：丢弃尾帧
		}

		rec := Record{Seq: seq, Data: body[kindLen : kindLen+dataLen]}
		rec.Kind = string(body[:kindLen])
		if emit != nil {
			if err := emit(rec); err != nil {
				return 0, 0, err
			}
		}
		off += int64(len(header)) + int64(len(body))
		lastSeq = seq
	}
}

// Close 同步、释放文件锁并关闭文件。
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	var err error
	if syncErr := w.f.Sync(); syncErr != nil {
		err = syncErr
	}
	if w.locked {
		if lerr := syscall.Flock(int(w.f.Fd()), syscall.LOCK_UN); lerr != nil && err == nil {
			err = lerr
		}
		w.locked = false
	}
	if cerr := w.f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	w.f = nil
	return err
}
