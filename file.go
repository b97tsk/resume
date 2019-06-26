package main

import (
	"encoding/gob"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

const (
	pieceSize = 1024 * 1024
	filePerm  = 0644
)

type DataFile struct {
	mu              sync.Mutex
	name            string
	file            *os.File
	hash            HashInfo
	incomplete      RangeSet
	completeSize    int64
	recentIncrement int64
	autoSyncSize    int64
}

type HashInfo struct {
	Pieces             []PieceInfo
	ContentSize        int64
	ContentMD5         string
	ContentDisposition string
	EntityTag          string
	LastModified       string
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
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, filePerm)
	if err == nil {
		f = &DataFile{name: name, file: file}
		f.incomplete.AddRange(0, math.MaxInt64)
	}
	return
}

func (f *DataFile) LoadHashFile() (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	name := filepath.Join(filepath.Dir(f.name), "Hash")
	file, err := os.Open(name)
	if err != nil {
		return
	}
	defer file.Close()

	var hash HashInfo
	err = gob.NewDecoder(file).Decode(&hash)
	if err != nil {
		return
	}

	var incomplete RangeSet
	var completeSize int64
	incomplete.AddRange(0, math.MaxInt64)

	for i := range hash.Pieces {
		p := &hash.Pieces[i]
		if p.Size > 0 && p.Size <= pieceSize && p.HashCode > 0 {
			offset := pieceSize * int64(i)
			incomplete.DeleteRange(offset, offset+int64(p.Size))
			completeSize += int64(p.Size)
			continue
		}
		p.Reset()
	}

	f.hash = hash
	f.incomplete = incomplete
	f.completeSize = completeSize

	if hash.ContentSize > 0 {
		f.setContentSizeLocked(hash.ContentSize)
	}
	return
}

func (f *DataFile) Incomplete() RangeSet {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append(RangeSet(nil), f.incomplete...)
}

func (f *DataFile) CompleteSize() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.completeSize
}

func (f *DataFile) SetContentSize(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.setContentSizeLocked(size)
}

func (f *DataFile) setContentSizeLocked(size int64) error {
	f.hash.ContentSize = size

	pieceCount := int(math.Ceil(float64(size) / pieceSize))
	if len(f.hash.Pieces) != pieceCount {
		pieces := f.hash.Pieces
		f.hash.Pieces = make([]PieceInfo, pieceCount)
		copy(f.hash.Pieces, pieces)
	}

	f.incomplete.DeleteRange(size, math.MaxInt64)

	if fileSize, _ := f.file.Seek(0, io.SeekEnd); size == fileSize {
		return nil
	}

	return f.file.Truncate(size)
}

func (f *DataFile) ContentSize() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hash.ContentSize
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

func (f *DataFile) SetContentDisposition(contentDisposition string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.ContentDisposition = contentDisposition
}

func (f *DataFile) ContentDisposition() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hash.ContentDisposition
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

func (f *DataFile) SetRange(s RangeSet) {
	f.mu.Lock()
	defer f.mu.Unlock()
	low := int64(math.MinInt64)
	for _, r := range s {
		f.incomplete.DeleteRange(low, r.Low)
		low = r.High
	}
	f.incomplete.DeleteRange(low, math.MaxInt64)
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
	if f.hash.ContentSize > 0 {
		f.incomplete.DeleteRange(f.hash.ContentSize, math.MaxInt64)
	}
}

func (f *DataFile) WriteAt(b []byte, offset int64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	n, err := f.file.WriteAt(b, offset)
	if err != nil {
		panic(err)
	}

	i := int(offset / pieceSize)
	p := &f.hash.Pieces[i]
	if offset != pieceSize*int64(i)+int64(p.Size) {
		panic("WriteAt failed")
	}

	f.incomplete.DeleteRange(offset, offset+int64(n))

	completeSize := f.completeSize
	defer func() {
		f.recentIncrement += f.completeSize - completeSize
		if f.autoSyncSize > 0 && f.recentIncrement >= f.autoSyncSize {
			f.recentIncrement = 0
			f.syncLocked()
		}
	}()

	pieceSizeRequired := pieceSize - int(p.Size)
	if n < pieceSizeRequired {
		f.completeSize += int64(n)
		p.HashCode = crc32.Update(p.HashCode, crc32.IEEETable, b)
		p.Size += uint32(n)
		return
	}

	f.completeSize += int64(pieceSizeRequired)
	p.HashCode = crc32.Update(p.HashCode, crc32.IEEETable, b[:pieceSizeRequired])
	p.Size = pieceSize
	b, i = b[pieceSizeRequired:], i+1

	for len(b) >= pieceSize {
		p := &f.hash.Pieces[i]
		if p.Size < pieceSize {
			f.completeSize += pieceSize - int64(p.Size)
			p.HashCode = crc32.ChecksumIEEE(b[:pieceSize])
			p.Size = pieceSize
		}
		b, i = b[pieceSize:], i+1
	}

	if len(b) > 0 {
		p := &f.hash.Pieces[i]
		if int(p.Size) < len(b) {
			f.completeSize += int64(len(b) - int(p.Size))
			p.HashCode = crc32.ChecksumIEEE(b)
			p.Size = uint32(len(b))
		}
	}
}

func (f *DataFile) Verify(digest io.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var (
		offset   int64
		hashCode uint32
	)

	w := func(b []byte) (n int, err error) {
		if digest != nil {
			n, err = digest.Write(b)
		} else {
			n, err = len(b), nil
		}
		if err != nil {
			return
		}

		i := int(offset / pieceSize)
		pieceSizeRequired := int(pieceSize*int64(i+1) - offset)
		if n < pieceSizeRequired {
			offset += int64(n)
			hashCode = crc32.Update(hashCode, crc32.IEEETable, b)
			return
		}

		p := &f.hash.Pieces[i]
		hashCode = crc32.Update(hashCode, crc32.IEEETable, b[:pieceSizeRequired])
		if hashCode != p.HashCode {
			offset := pieceSize * int64(i)
			f.incomplete.AddRange(offset, offset+int64(p.Size))
			f.completeSize -= int64(p.Size)
			p.Reset()
		}
		b, i = b[pieceSizeRequired:], i+1

		for len(b) >= pieceSize {
			p := &f.hash.Pieces[i]
			hashCode = crc32.ChecksumIEEE(b[:pieceSize])
			if hashCode != p.HashCode {
				offset := pieceSize * int64(i)
				f.incomplete.AddRange(offset, offset+int64(p.Size))
				f.completeSize -= int64(p.Size)
				p.Reset()
			}
			b, i = b[pieceSize:], i+1
		}

		offset += int64(n)
		hashCode = crc32.ChecksumIEEE(b)
		return
	}

	f.file.Seek(0, io.SeekStart)
	_, err := io.Copy(WriterFunc(w), f.file)

	if err == nil && hashCode > 0 {
		i := int(offset / pieceSize)
		p := &f.hash.Pieces[i]
		if hashCode != p.HashCode {
			offset := pieceSize * int64(i)
			f.incomplete.AddRange(offset, offset+int64(p.Size))
			f.completeSize -= int64(p.Size)
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
	name := filepath.Join(filepath.Dir(f.name), "Hash")
	file, err := os.OpenFile(name+"New", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	defer os.Remove(name + "New")

	err1 := gob.NewEncoder(file).Encode(&f.hash)
	err2 := file.Sync()
	err3 := file.Close()
	err4 := f.file.Sync()

	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	if err3 != nil {
		return err3
	}

	os.Rename(name+"New", name)

	return err4
}

func (f *DataFile) SetAutoSyncSize(size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.autoSyncSize = size
}

func (f *DataFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var err1 error
	if f.recentIncrement > 0 {
		f.recentIncrement = 0
		err1 = f.syncLocked()
	}

	err2 := f.file.Close()

	if err1 != nil {
		return err1
	}
	return err2
}

type WriterFunc func(p []byte) (n int, err error)

func (f WriterFunc) Write(p []byte) (n int, err error) {
	return f(p)
}
