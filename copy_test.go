package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

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

// A copy that is killed outright must leave nothing at the destination name.
// Everything that decides what still needs copying goes by that name alone, so
// a truncated file sitting there would pass for a finished one forever: never
// re-copied, never verified, and eventually hashed into checksums.b3 as if the
// short bytes were the footage.
//
// The source is a fifo, which pins the timing: job cannot open it until this
// goroutine does, and the write below cannot return until job has read it —
// which is after the destination file, whatever its name, has been created.
func TestJobLeavesNothingAtTheDestinationMidCopy(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "card.fifo")
	if err := syscall.Mkfifo(src, 0644); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}
	dst := filepath.Join(dir, "001_Mission", "GX010001.MP4")

	payload := make([]byte, copyBufSize+4096)
	fill(payload, 31)

	done := make(chan *result, 1)
	go func() { done <- job(src, dst, nil) }()

	w, err := os.OpenFile(src, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(payload[:copyBufSize]); err != nil {
		t.Fatal(err)
	}

	// Mid-copy: the bytes written so far are on the disk under some name, and
	// that name must not be the destination's.
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Error("a copy in flight is sitting at the destination name")
	}
	if _, err := os.Stat(partPath(dst)); err != nil {
		t.Errorf("no temporary at %s mid-copy: %v", partPath(dst), err)
	}

	if _, err := w.Write(payload[copyBufSize:]); err != nil {
		t.Fatal(err)
	}
	w.Close()

	var r *result
	select {
	case r = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("copy never finished")
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("nothing at the destination after a finished copy: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("destination content differs from source")
	}
	if r.srcHash != hex.EncodeToString([]byte(refHash(payload))) {
		t.Error("reported hash differs from hashing the source directly")
	}
	if _, err := os.Stat(partPath(dst)); !os.IsNotExist(err) {
		t.Error("the temporary outlived the copy")
	}
}

// The rename must not cost the destination the source's permissions, which are
// applied to the temporary while it still has its own name.
func TestJobKeepsSourceModeThroughTheRename(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.MP4")
	if err := os.WriteFile(src, []byte("footage"), 0640); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(dir, "out", "in.MP4")
	if r := job(src, dst, nil); r.err != nil {
		t.Fatal(r.err)
	}
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0640 {
		t.Errorf("destination mode %v, want %v", info.Mode().Perm(), os.FileMode(0640))
	}
}

// While it exists a temporary must be invisible to the scans — findFiles is how
// -checksum, -sync and -list all see a mission — and the next run into that
// mission must clear it, since nothing else ever will.
func TestPartFilesAreUnseenAndThenSwept(t *testing.T) {
	dir := t.TempDir()
	keep := filepath.Join(dir, "GX010001.MP4")
	drop := partPath(filepath.Join(dir, "GX010002.MP4"))
	for _, f := range []string{keep, drop} {
		if err := os.WriteFile(f, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	files, err := findFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].rel != "GX010001.MP4" {
		t.Errorf("scan returned %v, want the one finished clip", files)
	}

	// The same directory twice: a mission reached by two jobs is swept once.
	if n := sweepCopyParts([]string{dir, dir}); n != 1 {
		t.Errorf("swept %d file(s), want 1", n)
	}
	if _, err := os.Stat(drop); !os.IsNotExist(err) {
		t.Error("the leftover temporary survived the sweep")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Error("the sweep took a finished file with it")
	}
}
