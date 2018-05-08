package main

import (
	"encoding/gob"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

const (
	_pieceSize = 1024 * 1024
	_filePerm  = 0644
	_syncSize  = 16 * 1024 * 1024
)

type _DataFile struct {
	mu   sync.Mutex
	name string
	file *os.File
	hash struct {
		Pieces       []_PieceInfo
		FileSize     int64
		FileMD5      string
		EntityTag    string
		LastModified string
	}
	incomplete      _RangeSet
	completeSize    int64
	recentIncrement int
}

type _PieceInfo struct {
	Size     uint32
	HashCode uint32
}

func _openDataFile(name string) (f *_DataFile, err error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, _filePerm)
	if err == nil {
		f = &_DataFile{name: name, file: file}
		f.incomplete.AddRange(0, math.MaxInt64)
	}
	return
}

func (f *_DataFile) LoadHashFile() (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	name := filepath.Join(filepath.Dir(f.name), "Hash")
	file, err := os.Open(name + "New")
	if err != nil {
		file, err = os.Open(name)
	} else {
		os.Rename(name+"New", name)
	}
	if err != nil {
		return
	}
	defer file.Close()

	hash := f.hash
	err = gob.NewDecoder(file).Decode(&hash)
	if err != nil {
		return
	}

	var incomplete _RangeSet
	var completeSize int64
	incomplete.AddRange(0, math.MaxInt64)

	var buf [_pieceSize]byte
	for i := range hash.Pieces {
		p := &hash.Pieces[i]
		if p.Size > 0 && p.Size <= _pieceSize && p.HashCode > 0 {
			offset := _pieceSize * int64(i)
			f.file.Seek(offset, io.SeekStart)
			_, err := io.ReadFull(f.file, buf[:p.Size])
			if err == nil && crc32.ChecksumIEEE(buf[:p.Size]) == p.HashCode {
				incomplete.DeleteRange(offset, offset+int64(p.Size))
				completeSize += int64(p.Size)
				continue
			}
		}
		p.Size = 0
		p.HashCode = 0
	}

	f.hash = hash
	f.incomplete = incomplete
	f.completeSize = completeSize

	if hash.FileSize > 0 {
		f.setFileSizeLocked(hash.FileSize)
	}
	return
}

func (f *_DataFile) CompleteSize() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.completeSize
}

func (f *_DataFile) SetFileSize(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.setFileSizeLocked(size)
}

func (f *_DataFile) setFileSizeLocked(size int64) error {
	f.hash.FileSize = size

	pieceCount := int(math.Ceil(float64(size) / _pieceSize))
	if len(f.hash.Pieces) != pieceCount {
		pieces := f.hash.Pieces
		f.hash.Pieces = make([]_PieceInfo, pieceCount)
		copy(f.hash.Pieces, pieces)
	}

	f.incomplete.DeleteRange(size, math.MaxInt64)

	if fileSize, _ := f.file.Seek(0, io.SeekEnd); size == fileSize {
		return nil
	}

	return f.file.Truncate(size)
}

func (f *_DataFile) FileSize() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hash.FileSize
}

func (f *_DataFile) SetFileMD5(fileMD5 string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.FileMD5 = fileMD5
}

func (f *_DataFile) FileMD5() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hash.FileMD5
}

func (f *_DataFile) SetEntityTag(entityTag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.EntityTag = entityTag
}

func (f *_DataFile) EntityTag() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hash.EntityTag
}

func (f *_DataFile) SetLastModified(lastModified string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash.LastModified = lastModified
}

func (f *_DataFile) LastModified() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hash.LastModified
}

func (f *_DataFile) TakeIncomplete(max int64) (offset, size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.incomplete) == 0 {
		return
	}
	if max < _pieceSize {
		max = _pieceSize
	}
	r := f.incomplete[0]
	low, high := r.Low, (r.Low+max)/_pieceSize*_pieceSize
	if low >= high {
		return
	}
	if high > r.High {
		high = r.High
	}
	f.incomplete.DeleteRange(low, high)
	return low, high - low
}

func (f *_DataFile) ReturnIncomplete(offset, size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.incomplete.AddRange(offset, offset+size)
	if f.hash.FileSize > 0 {
		f.incomplete.DeleteRange(f.hash.FileSize, math.MaxInt64)
	}
}

func (f *_DataFile) WriteAt(b []byte, offset int64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	n, err := f.file.WriteAt(b, offset)
	if err != nil {
		panic(err)
	}

	i := int(offset / _pieceSize)
	p := &f.hash.Pieces[i]
	if offset != _pieceSize*int64(i)+int64(p.Size) {
		panic("WriteAt failed")
	}

	f.incomplete.DeleteRange(offset, offset+int64(n))

	completeSize := f.completeSize
	defer func() {
		f.recentIncrement += int(f.completeSize - completeSize)
		if f.recentIncrement >= _syncSize {
			f.recentIncrement = 0
			f.syncLocked()
		}
	}()

	pieceSizeRequired := _pieceSize - int(p.Size)
	if n < pieceSizeRequired {
		f.completeSize += int64(n)
		p.HashCode = crc32.Update(p.HashCode, crc32.IEEETable, b)
		p.Size += uint32(n)
		return
	}

	f.completeSize += int64(pieceSizeRequired)
	p.HashCode = crc32.Update(p.HashCode, crc32.IEEETable, b[:pieceSizeRequired])
	p.Size = _pieceSize
	b, i = b[pieceSizeRequired:], i+1

	for len(b) >= _pieceSize {
		p := &f.hash.Pieces[i]
		if p.Size < _pieceSize {
			f.completeSize += _pieceSize - int64(p.Size)
			p.HashCode = crc32.ChecksumIEEE(b[:_pieceSize])
			p.Size = _pieceSize
		}
		b, i = b[_pieceSize:], i+1
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

func (f *_DataFile) Checksum(digest hash.Hash) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.file.Seek(0, io.SeekStart)
	_, err := io.Copy(digest, f.file)
	return err
}

func (f *_DataFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recentIncrement > 0 {
		f.recentIncrement = 0
		return f.syncLocked()
	}
	return nil
}

func (f *_DataFile) SyncNow() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncLocked()
}

func (f *_DataFile) syncLocked() error {
	name := filepath.Join(filepath.Dir(f.name), "Hash")
	file, err := os.OpenFile(name+"New", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, _filePerm)
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

func (f *_DataFile) Close() error {
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
