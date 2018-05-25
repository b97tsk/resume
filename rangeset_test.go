package main

import (
	"fmt"
	"testing"
)

func TestRangeSet(t *testing.T) {
	var s RangeSet

	expect := func(title, expected string) {
		if fmt.Sprint(s) != expected {
			t.Fatal(title)
		}
	}

	s.AddRange(0, 0)
	expect("case 1", "[]")
	s.Add(1)
	expect("case 2", "[{1 2}]")
	s.Add(0)
	expect("case 3", "[{0 2}]")
	s.Add(2)
	expect("case 4", "[{0 3}]")
	s.Add(1)
	expect("case 5", "[{0 3}]")
	s.AddRange(4, 7)
	expect("case 6", "[{0 3} {4 7}]")
	s.DeleteRange(0, 0)
	expect("case 7", "[{0 3} {4 7}]")
	s.DeleteRange(10, 20)
	expect("case 8", "[{0 3} {4 7}]")
	s.Delete(1)
	expect("case 9", "[{0 1} {2 3} {4 7}]")
	s.Add(1)
	expect("case 10", "[{0 3} {4 7}]")
	s.Delete(1)
	s.DeleteRange(2, 6)
	expect("case 11", "[{0 1} {6 7}]")
	s.Reset()
	expect("case 12", "[]")
}
