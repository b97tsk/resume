package main

import (
	"math"
	"sort"
)

type Range struct {
	Low, High int64
}

type RangeSet []Range

func (p *RangeSet) Add(single int64) {
	p.AddRange(single, single+1)
}

func (p *RangeSet) AddRange(low, high int64) {
	s := *p
	i := sort.Search(len(s), func(i int) bool { return s[i].Low > low })
	j := sort.Search(len(s), func(i int) bool { return s[i].High > high })
	if i > j {
		return
	}
	if i > 0 && low <= s[i-1].High {
		low = s[i-1].Low
		i--
	}
	if j < len(s) && high >= s[j].Low {
		high = s[j].High
		j++
	}
	if i == j {
		if low < high {
			s = append(s, Range{})
			copy(s[i+1:], s[i:])
			s[i] = Range{low, high}
			*p = s
		}
		return
	}
	s[i] = Range{low, high}
	*p = append(s[:i+1], s[j:]...)
}

func (p *RangeSet) Delete(single int64) {
	p.DeleteRange(single, single+1)
}

func (p *RangeSet) DeleteRange(low, high int64) {
	s := *p
	i := sort.Search(len(s), func(i int) bool { return s[i].High > low })
	t := s[i:]
	j := i + sort.Search(len(t), func(i int) bool { return t[i].Low > high })
	if i == j {
		return
	}
	if i == j-1 {
		if low > s[i].Low {
			if high < s[i].High {
				if low < high {
					s = append(s, Range{})
					copy(s[j:], s[i:])
					s[i].High = low
					s[j].Low = high
					*p = s
				}
			} else {
				s[i].High = low
			}
		} else {
			if high < s[i].High {
				s[i].Low = high
			} else {
				*p = append(s[:i], s[j:]...)
			}
		}
		return
	}
	if low > s[i].Low {
		s[i].High = low
		i++
	}
	if high < s[j-1].High {
		s[j-1].Low = high
		j--
	}
	*p = append(s[:i], s[j:]...)
}

func (s RangeSet) Contains(single int64) bool {
	return s.ContainsRange(single, single+1)
}

func (s RangeSet) ContainsRange(low, high int64) bool {
	i := sort.Search(len(s), func(i int) bool { return s[i].High > low })
	return i < len(s) && s[i].Low <= low && high <= s[i].High
}

func (s RangeSet) ContainsAny(low, high int64) bool {
	i := sort.Search(len(s), func(i int) bool { return s[i].High > low })
	t := s[i:]
	return sort.Search(len(t), func(i int) bool { return t[i].Low >= high }) > 0
}

func (s RangeSet) Equals(other RangeSet) bool {
	if len(s) != len(other) {
		return false
	}
	for i, r := range s {
		if r != other[i] {
			return false
		}
	}
	return true
}

func (s RangeSet) Intersect(other RangeSet) RangeSet {
	var result RangeSet
	for {
		if len(s) < len(other) {
			s, other = other, s
		}
		if len(other) == 0 {
			break
		}
		r := other[0]
		i := sort.Search(len(s), func(i int) bool { return s[i].High > r.Low })
		t := s[i:]
		j := i + sort.Search(len(t), func(i int) bool { return t[i].Low >= r.High })
		if i < j {
			start := len(result)
			result = append(result, s[i:j]...)
			if r0 := &result[start]; r0.Low < r.Low {
				r0.Low = r.Low
			}
			if r1 := &result[len(result)-1]; r1.High > r.High {
				r1.High = r.High
			}
			j--
		}
		s, other = s[j:], other[1:]
	}
	return result
}

func (s RangeSet) Inverse() RangeSet {
	if len(s) == 0 {
		return RangeSet{{math.MinInt64, math.MaxInt64}}
	}
	var result RangeSet
	r0 := s[0]
	if r0.Low > math.MinInt64 {
		result = append(result, Range{math.MinInt64, r0.Low})
	}
	low := r0.High
	for _, r := range s[1:] {
		result = append(result, Range{low, r.Low})
		low = r.High
	}
	if low < math.MaxInt64 {
		result = append(result, Range{low, math.MaxInt64})
	}
	return result
}

func (s RangeSet) Sum() uint64 {
	acc := uint64(0)
	for _, r := range s {
		acc += uint64(r.High - r.Low)
	}
	return acc
}
