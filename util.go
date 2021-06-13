package main

type ReaderFunc func(p []byte) (n int, err error)

func (f ReaderFunc) Read(p []byte) (n int, err error) {
	return f(p)
}

type ReaderAtFunc func(p []byte, off int64) (n int, err error)

func (f ReaderAtFunc) ReadAt(p []byte, off int64) (n int, err error) {
	return f(p, off)
}

type WriterFunc func(p []byte) (n int, err error)

func (f WriterFunc) Write(p []byte) (n int, err error) {
	return f(p)
}
