package main

type ReaderFunc func(p []byte) (n int, err error)

func (f ReaderFunc) Read(p []byte) (n int, err error) {
	return f(p)
}

type WriterFunc func(p []byte) (n int, err error)

func (f WriterFunc) Write(p []byte) (n int, err error) {
	return f(p)
}

type SeekerFunc func(offset int64, whence int) (int64, error)

func (f SeekerFunc) Seek(offset int64, whence int) (int64, error) {
	return f(offset, whence)
}
