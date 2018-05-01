package main

import (
	"math"
	"sort"
)

type _Range struct {
	X, Y int64
}

type _RangeSet []_Range

func (s *_RangeSet) Add(x int64) {
	s.AddRange(x, x)
}

func (s *_RangeSet) AddRange(x, y int64) {
	if x > y {
		return
	}
	i := sort.Search(len(*s), func(i int) bool { return (*s)[i].X > x }) - 1
	j := sort.Search(len(*s), func(i int) bool { return (*s)[i].Y > y })
	if i >= j {
		return
	}
	var r _Range
	var low, high int
	if i >= 0 && x <= _tryInc((*s)[i].Y) {
		r.X = (*s)[i].X
		low = i
	} else {
		r.X = x
		low = i + 1
	}
	if j < len(*s) && (*s)[j].X <= _tryInc(y) {
		r.Y = (*s)[j].Y
		high = j + 1
	} else {
		r.Y = y
		high = j
	}
	if low < high {
		(*s)[low] = r
	} else {
		*s = append(*s, _Range{})
		copy((*s)[low+1:], (*s)[low:])
		(*s)[low] = r
	}
	low++
	if low < high {
		*s = append((*s)[:low], (*s)[high:]...)
	}
}

func (s *_RangeSet) Delete(x int64) {
	s.DeleteRange(x, x)
}

func (s *_RangeSet) DeleteRange(x, y int64) {
	if x > y {
		return
	}
	i := sort.Search(len(*s), func(i int) bool { return (*s)[i].X > x }) - 1
	j := sort.Search(len(*s), func(i int) bool { return (*s)[i].Y > y })
	if i > j {
		return
	}
	var r1, r2 _Range
	var low, high int
	if i >= 0 && x <= _tryInc((*s)[i].Y) {
		r1.X, r1.Y = (*s)[i].X, _tryDec(x)
		low = i
	} else {
		r1.X, r1.Y = 1, 0
		low = i + 1
	}
	if j < len(*s) && (*s)[j].X <= _tryInc(y) {
		r2.X, r2.Y = _tryInc(y), (*s)[j].Y
		high = j + 1
	} else {
		r2.X, r2.Y = 1, 0
		high = j
	}
	if r1.X <= r1.Y {
		if low < high {
			(*s)[low] = r1
		} else {
			*s = append(*s, _Range{})
			copy((*s)[low+1:], (*s)[low:])
			(*s)[low] = r1
		}
		low++
	}
	if r2.X <= r2.Y {
		if low < high {
			(*s)[low] = r2
		} else {
			*s = append(*s, _Range{})
			copy((*s)[low+1:], (*s)[low:])
			(*s)[low] = r2
		}
		low++
	}
	if low < high {
		*s = append((*s)[:low], (*s)[high:]...)
	}
}

func (s *_RangeSet) Reset() {
	*s = nil
}

func _tryDec(x int64) int64 {
	if x > math.MinInt64 {
		return x - 1
	}
	return x
}

func _tryInc(x int64) int64 {
	if x < math.MaxInt64 {
		return x + 1
	}
	return x
}
