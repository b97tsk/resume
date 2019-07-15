package main

import (
	"fmt"
	"testing"
)

func TestRangeSet(t *testing.T) {
	var s RangeSet

	assert := func(title string, ok bool) {
		if !ok {
			t.Fatal(title)
		}
	}

	s.AddRange(0, 0)
	assert("case 1", fmt.Sprint(s) == "[]")
	s.Add(1)
	assert("case 2", fmt.Sprint(s) == "[{1 2}]")
	s.Add(0)
	assert("case 3", fmt.Sprint(s) == "[{0 2}]")
	s.Add(2)
	assert("case 4", fmt.Sprint(s) == "[{0 3}]")
	s.Add(1)
	assert("case 5", fmt.Sprint(s) == "[{0 3}]")
	s.AddRange(4, 7)
	assert("case 6", fmt.Sprint(s) == "[{0 3} {4 7}]")
	s.DeleteRange(0, 0)
	assert("case 7", fmt.Sprint(s) == "[{0 3} {4 7}]")
	s.DeleteRange(10, 20)
	assert("case 8", fmt.Sprint(s) == "[{0 3} {4 7}]")
	s.Delete(1)
	assert("case 9", fmt.Sprint(s) == "[{0 1} {2 3} {4 7}]")
	s.Add(1)
	assert("case 10", fmt.Sprint(s) == "[{0 3} {4 7}]")
	s.Delete(1)
	s.DeleteRange(2, 6)
	assert("case 11", fmt.Sprint(s) == "[{0 1} {6 7}]")
	s.Reset()
	assert("case 12", fmt.Sprint(s) == "[]")

	assert("case 13", !s.ContainsRange(0, 0))
	assert("case 14", !s.ContainsAny(0, 0))
	s.Add(1)
	assert("case 15", !s.Contains(0))
	assert("case 16", s.Contains(1))
	assert("case 17", !s.ContainsRange(0, 3))
	assert("case 18", s.ContainsAny(0, 3))
	s.Add(0)
	assert("case 19", s.Contains(0))
	assert("case 20", s.Contains(1))
	assert("case 21", !s.ContainsRange(0, 3))
	assert("case 22", s.ContainsAny(0, 3))
	s.Add(2)
	assert("case 23", s.Contains(0))
	assert("case 24", s.Contains(1))
	assert("case 25", s.Contains(2))
	assert("case 26", s.ContainsRange(0, 3))
	assert("case 27", s.ContainsAny(0, 3))
	s.Delete(1)
	assert("case 28", s.Contains(0))
	assert("case 29", !s.Contains(1))
	assert("case 30", s.Contains(2))
	assert("case 31", !s.ContainsRange(0, 3))
	assert("case 32", s.ContainsAny(0, 3))
}
