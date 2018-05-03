package main

import (
	"sort"
)

type _Range struct {
	Low, High int64
}

type _RangeSet []_Range

func (s *_RangeSet) Add(single int64) {
	s.AddRange(single, single+1)
}

func (s *_RangeSet) AddRange(low, high int64) {
	if low >= high {
		return
	}

	i := sort.Search(len(*s), func(i int) bool { return (*s)[i].Low > low }) - 1
	j := sort.Search(len(*s), func(i int) bool { return (*s)[i].High > high })
	if i == j {
		return
	}

	var r _Range
	if i >= 0 && low <= (*s)[i].High {
		r.Low = (*s)[i].Low
	} else {
		r.Low = low
		i++
	}
	if j < len(*s) && (*s)[j].Low <= high {
		r.High = (*s)[j].High
		j++
	} else {
		r.High = high
	}

	if i < j {
		(*s)[i] = r
	} else {
		*s = append(*s, _Range{})
		copy((*s)[i+1:], (*s)[i:])
		(*s)[i] = r
	}
	i++

	if i < j {
		*s = append((*s)[:i], (*s)[j:]...)
	}
}

func (s *_RangeSet) Delete(single int64) {
	s.DeleteRange(single, single+1)
}

func (s *_RangeSet) DeleteRange(low, high int64) {
	if low >= high {
		return
	}

	i := sort.Search(len(*s), func(i int) bool { return (*s)[i].Low > low }) - 1
	j := sort.Search(len(*s), func(i int) bool { return (*s)[i].High > high })

	var r1, r2 _Range
	if i >= 0 && low <= (*s)[i].High {
		r1.Low, r1.High = (*s)[i].Low, low
	} else {
		i++
	}
	if j < len(*s) && (*s)[j].Low <= high {
		r2.Low, r2.High = high, (*s)[j].High
		j++
	}

	if r1.Low < r1.High {
		(*s)[i] = r1
		i++
	}
	if r2.Low < r2.High {
		if i < j {
			(*s)[i] = r2
		} else {
			*s = append(*s, _Range{})
			copy((*s)[i+1:], (*s)[i:])
			(*s)[i] = r2
		}
		i++
	}

	if i < j {
		*s = append((*s)[:i], (*s)[j:]...)
	}
}

func (s *_RangeSet) Reset() {
	*s = nil
}
