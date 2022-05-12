package main

import (
	"archive/zip"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/b97tsk/rangeset"
)

const (
	pieceSize   = 1024 * 1024
	zipFileName = "e29b33b3-7e9c-40e0-a41b-65cdadf3b068"
)

type DataFile struct {
	mu              sync.Mutex
	file            *os.File
	hash            HashInfo
	completed       rangeset.RangeSet[int64]
	incomplete      rangeset.RangeSet[int64]
	requested       rangeset.RangeSet[int64]
	ignoreSize      *int64
	contentSize     *int64
	completeSize    *int64
	recentIncrement int64
	autoSyncSize    int64
}

type HashInfo struct {
	Pieces        []PieceInfo
	ContentSize   int64
	ContentMD5    string
	ContentSHA1   string
	ContentSHA224 string
	ContentSHA256 string
	ContentSHA384 string
	ContentSHA512 string
	EntityTag     string
	LastModified  string
	Filename      string
}

type PieceInfo struct {
	Size     uint32
	HashCode uint32
}

func (p *PieceInfo) Reset() {
	p.Size = 0
	p.HashCode = 0
}

func openDataFile(name string) (f *DataFile, err error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return
	}

	var ints [3]int64 // Ensure 64-bit alignment for atomic operations.

	f = &DataFile{
		file:         file,
		incomplete:   rangeset.FromRange[int64](0, math.MaxInt64),
		ignoreSize:   &ints[0],
		contentSize:  &ints[1],
		completeSize: &ints[2],
	}

	return
}

func (f *DataFile) HashFile() string {
	return f.file.Name() + ".resume"
}

func (f *DataFile) TempHashFile() string {
	return f.file.Name() + ".resume~"
}

func (f *DataFile) LoadHashFile() (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := os.Open(f.HashFile())
	if err != nil {
		return
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return
	}

	zr, err := zip.NewReader(file, fi.Size())
	if err != nil {
		return f.loadHashFileFailed()
	}

	if len(zr.File) != 1 || zr.File[0].Name != zipFileName {
		return f.loadHashFileFailed()
	}

	rc, err := zr.File[0].Open()
	if err != nil {
		return f.loadHashFileFailed()
	}
	defer rc.Close()

	var hash HashInfo

	if err := gob.NewDecoder(rc).Decode(&hash); err != nil {
		return f.loadHashFileFailed()
	}

	var completed rangeset.RangeSet[int64]

	for i := range hash.Pieces {
		p := &hash.Pieces[i]
		if p.Size > 0 && p.Size <= pieceSize && p.HashCode > 0 {
			offset := pieceSize * int64(i)
			completed.AddRange(offset, offset+int64(p.Size))

			continue
		}

		p.Reset()
	}

	f.hash = hash
	f.completed = completed
	f.incomplete = rangeset.FromRange[int64](0, math.MaxInt64).Difference(completed)
	atomic.StoreInt64(f.completeSize, int64(completed.Count()))

	if hash.ContentSize > 0 {
		f.setContentSizeLocked(hash.ContentSize)
	}

	return
}

func (f *DataFile) loadHashFileFailed() error {
	return fmt.Errorf("open %v: tampered or incompatible", f.HashFile())
}

func (f *DataFile) Incomplete() rangeset.RangeSet[int64] {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.getIncompleteLocked()
}

func (f *DataFile) getIncompleteLocked() rangeset.RangeSet[int64] {
	high := int64(math.MaxInt64)
	if *f.contentSize > 0 {
		high = *f.contentSize
	}

	return rangeset.FromRange(0, high).Difference(f.completed)
}

func (f *DataFile) IncompleteSize() int64 {
	if contentSize := f.ContentSize(); contentSize > 0 {
		return contentSize - f.CompleteSize() - f.IgnoreSize()
	}

	return -1
}

func (f *DataFile) IgnoreSize() int64 {
	return atomic.LoadInt64(f.ignoreSize)
}

func (f *DataFile) CompleteSize() int64 {
	return atomic.LoadInt64(f.completeSize)
}

func (f *DataFile) SetContentSize(size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setContentSizeLocked(size)
}

func (f *DataFile) setContentSizeLocked(size int64) {
	f.hash.ContentSize = size
	atomic.StoreInt64(f.contentSize, size)

	pieceCount := int(math.Ceil(float64(size) / pieceSize))
	if len(f.hash.Pieces) != pieceCount {
		pieces := f.hash.Pieces
		f.hash.Pieces = make([]PieceInfo, pieceCount)
		copy(f.hash.Pieces, pieces)
	}

	f.incomplete.DeleteRange(size, math.MaxInt64)
	f.updateIgnoreSizeLocked()
}

func (f *DataFile) updateIgnoreSizeLocked() {
	ignoreSize := int64(0)

	if f.requested != nil {
		ignored := f.getIncompleteLocked().Difference(f.requested)
		ignoreSize = int64(ignored.Count())
	}

	atomic.StoreInt64(f.ignoreSize, ignoreSize)
}

func (f *DataFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	offsetSave, _ := f.file.Seek(0, io.SeekCurrent)

	defer func() {
		_, _ = f.file.Seek(offsetSave, io.SeekStart)
	}()

	fileSize, _ := f.file.Seek(0, io.SeekEnd)
	if size == fileSize {
		return nil
	}

	return f.file.Truncate(size)
}

func (f *DataFile) ContentSize() int64 {
	return atomic.LoadInt64(f.contentSize)
}

func (f *DataFile) SetContentMD5(contentMD5 string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.ContentMD5 = contentMD5
}

func (f *DataFile) ContentMD5() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.hash.ContentMD5
}

func (f *DataFile) SetContentSHA1(contentSHA1 string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.ContentSHA1 = contentSHA1
}

func (f *DataFile) ContentSHA1() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.hash.ContentSHA1
}

func (f *DataFile) SetContentSHA224(contentSHA224 string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.ContentSHA224 = contentSHA224
}

func (f *DataFile) ContentSHA224() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.hash.ContentSHA224
}

func (f *DataFile) SetContentSHA256(contentSHA256 string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.ContentSHA256 = contentSHA256
}

func (f *DataFile) ContentSHA256() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.hash.ContentSHA256
}

func (f *DataFile) SetContentSHA384(contentSHA384 string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.ContentSHA384 = contentSHA384
}

func (f *DataFile) ContentSHA384() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.hash.ContentSHA384
}

func (f *DataFile) SetContentSHA512(contentSHA512 string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.ContentSHA512 = contentSHA512
}

func (f *DataFile) ContentSHA512() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.hash.ContentSHA512
}

func (f *DataFile) SetEntityTag(entityTag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.EntityTag = entityTag
}

func (f *DataFile) EntityTag() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.hash.EntityTag
}

func (f *DataFile) SetLastModified(lastModified string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.LastModified = lastModified
}

func (f *DataFile) LastModified() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.hash.LastModified
}

func (f *DataFile) SetFilename(filename string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.Filename = filename
}

func (f *DataFile) Filename() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.hash.Filename
}

func (f *DataFile) SetRange(s rangeset.RangeSet[int64]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.incomplete = f.incomplete.Intersection(s)
	f.requested = append(rangeset.RangeSet[int64]{}, f.incomplete...)

	if *f.contentSize > 0 {
		f.updateIgnoreSizeLocked()
	}
}

func (f *DataFile) HasIncomplete() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.incomplete) > 0
}

func (f *DataFile) TakeIncomplete(max int64) (offset, size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.incomplete) == 0 {
		return
	}

	if max < pieceSize {
		max = pieceSize
	}

	r := f.incomplete[0]

	low, high := r.Low, (r.Low+max)/pieceSize*pieceSize
	if low >= high {
		return
	}

	if high > r.High {
		high = r.High
	}

	f.incomplete.DeleteRange(low, high)

	return low, high - low
}

func (f *DataFile) ReturnIncomplete(offset, size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.incomplete.AddRange(offset, offset+size)

	if *f.contentSize > 0 {
		f.incomplete.DeleteRange(*f.contentSize, math.MaxInt64)
	}
}

func (f *DataFile) ReadAt(b []byte, offset int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	i := sort.Search(len(f.completed), func(i int) bool {
		return f.completed[i].High > offset
	})

	if i < len(f.completed) && f.completed[i].Low <= offset {
		available := int(f.completed[i].High - offset)
		if available < len(b) {
			b = b[:available]
		}

		return f.file.ReadAt(b, offset)
	}

	return 0, ErrIncomplete
}

func (f *DataFile) WriteAt(b []byte, offset int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	n, err = f.file.WriteAt(b, offset)
	if err != nil {
		panic(err)
	}

	i := int(offset / pieceSize)
	p := &f.hash.Pieces[i]

	if offset != pieceSize*int64(i)+int64(p.Size) {
		panic("WriteAt failed")
	}

	f.completed.AddRange(offset, offset+int64(n))
	f.incomplete.DeleteRange(offset, offset+int64(n))

	completeSize := *f.completeSize

	defer func() {
		f.recentIncrement += *f.completeSize - completeSize
		if f.autoSyncSize > 0 && f.recentIncrement >= f.autoSyncSize {
			f.recentIncrement = 0
			_ = f.syncLocked()
		}
	}()

	pieceSizeRequired := pieceSize - int(p.Size)
	if n < pieceSizeRequired {
		atomic.AddInt64(f.completeSize, int64(n))

		p.HashCode = crc32.Update(p.HashCode, crc32.IEEETable, b)
		p.Size += uint32(n)

		return
	}

	atomic.AddInt64(f.completeSize, int64(pieceSizeRequired))
	p.HashCode = crc32.Update(p.HashCode, crc32.IEEETable, b[:pieceSizeRequired])
	p.Size = pieceSize
	b, i = b[pieceSizeRequired:], i+1

	for len(b) >= pieceSize {
		p := &f.hash.Pieces[i]
		if p.Size < pieceSize {
			atomic.AddInt64(f.completeSize, pieceSize-int64(p.Size))
			p.HashCode = crc32.ChecksumIEEE(b[:pieceSize])
			p.Size = pieceSize
		}

		b, i = b[pieceSize:], i+1
	}

	if len(b) > 0 {
		p := &f.hash.Pieces[i]
		if int(p.Size) < len(b) {
			atomic.AddInt64(f.completeSize, int64(len(b)-int(p.Size)))
			p.HashCode = crc32.ChecksumIEEE(b)
			p.Size = uint32(len(b))
		}
	}

	return
}

func (f *DataFile) Alloc(ctx context.Context, progress chan<- int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	const NB = 1024 * 1024
	buf := make([]byte, NB)
	done := ctx.Done()

	for _, r := range f.incomplete {
		if r.High == math.MaxInt64 {
			break
		}

		offset := r.Low
		_, _ = f.file.Seek(offset, io.SeekStart)

		size := r.High - r.Low

		for size > NB {
			n, err := f.file.Write(buf)
			if err != nil {
				return err
			}

			size -= int64(n)
			offset += int64(n)

			select {
			case <-done:
				return ctx.Err()
			case progress <- offset:
			default:
			}
		}

		_, err := f.file.Write(buf[:size])
		if err != nil {
			return err
		}

		select {
		case <-done:
			return ctx.Err()
		case progress <- r.High:
		default:
		}
	}

	return nil
}

func (f *DataFile) Verify(ctx context.Context, digest io.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var (
		offset   int64
		hashCode uint32
	)

	done := ctx.Done()

	w := func(b []byte) (n int, err error) {
		if digest != nil {
			n, err = digest.Write(b)
		} else {
			n, err = len(b), nil
		}

		if err != nil {
			return
		}

		select {
		case <-done:
			return 0, ctx.Err()
		default:
		}

		i := int(offset / pieceSize)

		pieceSizeRequired := int(pieceSize*int64(i+1) - offset)
		if n < pieceSizeRequired {
			offset += int64(n)
			hashCode = crc32.Update(hashCode, crc32.IEEETable, b)

			return
		}

		hashCode = crc32.Update(hashCode, crc32.IEEETable, b[:pieceSizeRequired])

		p := &f.hash.Pieces[i]
		if hashCode != p.HashCode {
			offset := pieceSize * int64(i)
			f.completed.DeleteRange(offset, offset+int64(p.Size))
			f.incomplete.AddRange(offset, offset+int64(p.Size))
			atomic.AddInt64(f.completeSize, -int64(p.Size))
			p.Reset()
		}

		b, i = b[pieceSizeRequired:], i+1

		for len(b) >= pieceSize {
			hashCode = crc32.ChecksumIEEE(b[:pieceSize])

			p := &f.hash.Pieces[i]
			if hashCode != p.HashCode {
				offset := pieceSize * int64(i)
				f.completed.DeleteRange(offset, offset+int64(p.Size))
				f.incomplete.AddRange(offset, offset+int64(p.Size))
				atomic.AddInt64(f.completeSize, -int64(p.Size))
				p.Reset()
			}

			b, i = b[pieceSize:], i+1
		}

		offset += int64(n)
		hashCode = crc32.ChecksumIEEE(b)

		return
	}

	var err error

	for _, r := range f.completed {
		offset, hashCode = r.Low, 0

		_, _ = f.file.Seek(offset, io.SeekStart)

		_, err = io.Copy(WriterFunc(w), io.LimitReader(f.file, r.High-r.Low))
		if err != nil {
			break
		}
	}

	if err == nil && hashCode > 0 {
		i := int(offset / pieceSize)

		p := &f.hash.Pieces[i]
		if hashCode != p.HashCode {
			offset := pieceSize * int64(i)
			f.completed.DeleteRange(offset, offset+int64(p.Size))
			f.incomplete.AddRange(offset, offset+int64(p.Size))
			atomic.AddInt64(f.completeSize, -int64(p.Size))
			p.Reset()
		}
	}

	return err
}

func (f *DataFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.recentIncrement > 0 {
		f.recentIncrement = 0
		return f.syncLocked()
	}

	return nil
}

func (f *DataFile) SyncNow() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.syncLocked()
}

func (f *DataFile) syncLocked() error {
	serr := f.file.Sync()

	file, err := os.Create(f.TempHashFile())
	if err != nil {
		return err
	}

	defer os.Remove(f.TempHashFile())

	zw := zip.NewWriter(file)

	w, err := zw.Create(zipFileName)
	if err == nil {
		err = gob.NewEncoder(w).Encode(&f.hash)
	}

	if cerr := zw.Close(); err == nil {
		err = cerr
	}

	if err == nil {
		err = file.Sync()
	}

	if cerr := file.Close(); err == nil {
		err = cerr
	}

	if err != nil {
		return err
	}

	if err := os.Rename(f.TempHashFile(), f.HashFile()); err != nil {
		return err
	}

	return serr
}

func (f *DataFile) SetAutoSyncSize(size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.autoSyncSize = size
}

func (f *DataFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var serr error

	if f.recentIncrement > 0 {
		f.recentIncrement = 0
		serr = f.syncLocked()
	}

	cerr := f.file.Close()

	if serr != nil {
		return serr
	}

	return cerr
}

var ErrIncomplete = errors.New("incomplete")
