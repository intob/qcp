package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/vbauerster/mpb/v8"

	"lukechampine.com/blake3"
)

// fill writes a deterministic, position-sensitive pattern, so a copy that
// drops, duplicates or reorders a chunk shows up as a content or hash mismatch.
func fill(b []byte, seed uint64) {
	x := seed*2862933555777941757 + 3037000493
	for i := range b {
		x = x*6364136223846793005 + 1442695040888963407
		b[i] = byte(x >> 33)
	}
}

func hexOf(h *blake3.Hasher) string { return hex.EncodeToString(h.Sum(nil)) }

func refHash(b []byte) string {
	h := blake3.New(32, nil)
	h.Write(b)
	return string(h.Sum(nil))
}

// Sizes either side of the buffer boundary: the last chunk of a real file is
// almost never a whole multiple of copyBufSize.
func TestCopyPipelinedSizes(t *testing.T) {
	sizes := []int{
		0, 1, 1023,
		copyBufSize - 1, copyBufSize, copyBufSize + 1,
		2 * copyBufSize, 3*copyBufSize + 7777,
	}
	for _, size := range sizes {
		src := make([]byte, size)
		fill(src, uint64(size))

		var out bytes.Buffer
		h := blake3.New(32, nil)
		n, err := copyPipelined(&out, bytes.NewReader(src), h)
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if n != int64(size) {
			t.Errorf("size %d: reported %d bytes written", size, n)
		}
		if !bytes.Equal(out.Bytes(), src) {
			t.Errorf("size %d: destination content differs from source", size)
		}
		if string(h.Sum(nil)) != refHash(src) {
			t.Errorf("size %d: hash differs from hashing the source directly", size)
		}
	}
}

// dribbleReader never fills the buffer it is given. ReadFull has to coalesce
// these, or the destination sees undersized writes.
type dribbleReader struct{ r io.Reader }

func (d dribbleReader) Read(p []byte) (int, error) {
	if len(p) > 7919 {
		p = p[:7919]
	}
	return d.r.Read(p)
}

// sizeCheckWriter fails if it is handed a short write before the final chunk.
type sizeCheckWriter struct {
	t     *testing.T
	buf   bytes.Buffer
	short bool // a short write was seen; another one after it is a bug
}

func (w *sizeCheckWriter) Write(p []byte) (int, error) {
	if w.short {
		w.t.Errorf("write of %d bytes followed a short write; chunks were not coalesced", len(p))
	}
	if len(p) != copyBufSize {
		w.short = true
	}
	return w.buf.Write(p)
}

func TestCopyPipelinedCoalescesShortReads(t *testing.T) {
	src := make([]byte, 5*copyBufSize+123)
	fill(src, 9)

	w := &sizeCheckWriter{t: t}
	h := blake3.New(32, nil)
	if _, err := copyPipelined(w, dribbleReader{bytes.NewReader(src)}, h); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(w.buf.Bytes(), src) {
		t.Error("destination content differs from source")
	}
	if string(h.Sum(nil)) != refHash(src) {
		t.Error("hash differs from hashing the source directly")
	}
}

var errRead = errors.New("read failed")

type failingReader struct {
	r    io.Reader
	at   int64
	seen int64
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.seen >= f.at {
		return 0, errRead
	}
	n, err := f.r.Read(p)
	f.seen += int64(n)
	return n, err
}

func TestCopyPipelinedReadError(t *testing.T) {
	src := make([]byte, 10*copyBufSize)
	r := &failingReader{r: bytes.NewReader(src), at: 2 * copyBufSize}
	_, err := copyPipelined(io.Discard, r, blake3.New(32, nil))
	if !errors.Is(err, errRead) {
		t.Fatalf("got %v, want %v", err, errRead)
	}
}

var errWrite = errors.New("write failed")

type failingWriter struct{ ok int }

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.ok <= 0 {
		return 0, errWrite
	}
	w.ok--
	return len(p), nil
}

// A destination giving up early must not strand the reader goroutine. Under
// -race this also catches the goroutine outliving the call.
func TestCopyPipelinedWriteError(t *testing.T) {
	src := make([]byte, 50*copyBufSize)
	_, err := copyPipelined(&failingWriter{ok: 3}, bytes.NewReader(src), blake3.New(32, nil))
	if !errors.Is(err, errWrite) {
		t.Fatalf("got %v, want %v", err, errWrite)
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestCopyPipelinedShortWrite(t *testing.T) {
	src := make([]byte, 4*copyBufSize)
	_, err := copyPipelined(shortWriter{}, bytes.NewReader(src), blake3.New(32, nil))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("got %v, want %v", err, io.ErrShortWrite)
	}
}

// hashFile must agree with the copy hash whether or not a progress bar wraps
// the reader — the nil-bar path is the one that used to fall back to 32 KiB
// reads via os.File.WriteTo.
func TestHashFileMatchesCopy(t *testing.T) {
	src := make([]byte, 2*copyBufSize+4242)
	fill(src, 77)
	path := filepath.Join(t.TempDir(), "sample.bin")
	if err := os.WriteFile(path, src, 0644); err != nil {
		t.Fatal(err)
	}

	want := blake3.New(32, nil)
	if _, err := copyPipelined(io.Discard, bytes.NewReader(src), want); err != nil {
		t.Fatal(err)
	}
	wantHex := hexOf(want)

	got, err := hashFile(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != wantHex {
		t.Errorf("hashFile (no bar) gave %s, want %s", got, wantHex)
	}

	// same file again, this time with the reader wrapped in a progress bar
	p := mpb.New(mpb.WithOutput(io.Discard))
	tracker := addBar(p, "test", int64(len(src)))
	got, err = hashFile(path, tracker)
	if err != nil {
		t.Fatal(err)
	}
	tracker.flush()
	p.Wait()
	if got != wantHex {
		t.Errorf("hashFile (with bar) gave %s, want %s", got, wantHex)
	}
}
