package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
)

func enew(text string) error {
	return errors.New(text)
}

func errorf(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}

func print(a ...interface{}) {
	fmt.Fprint(os.Stdout, a...)
}

func printf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stdout, format, a...)
}

func println(a ...interface{}) {
	fmt.Fprintln(os.Stdout, a...)
}

func eprint(a ...interface{}) {
	fmt.Fprint(os.Stderr, a...)
}

func eprintf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
}

func eprintln(a ...interface{}) {
	fmt.Fprintln(os.Stderr, a...)
}

func fprint(w io.Writer, a ...interface{}) {
	fmt.Fprint(w, a...)
}

func fprintf(w io.Writer, format string, a ...interface{}) {
	fmt.Fprintf(w, format, a...)
}

func fprintln(w io.Writer, a ...interface{}) {
	fmt.Fprintln(w, a...)
}

func sprint(a ...interface{}) string {
	return fmt.Sprint(a...)
}

func sprintf(format string, a ...interface{}) string {
	return fmt.Sprintf(format, a...)
}

func sprintln(a ...interface{}) string {
	return fmt.Sprintln(a...)
}

func itoa(i int) string {
	return strconv.Itoa(i)
}
