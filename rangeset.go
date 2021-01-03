package main

import (
	"math"
	"sort"
)

type Range struct {
	Low, High int64
}

type RangeSet []Range

func (set *RangeSet) Add(single int64) {
	set.AddRange(single, single+1)
}

func (set *RangeSet) AddRange(low, high int64) {
	s := *set
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
			*set = s
		}

		return
	}

	s[i] = Range{low, high}
	s = append(s[:i+1], s[j:]...)
	*set = s
}

func (set *RangeSet) Delete(single int64) {
	set.DeleteRange(single, single+1)
}

func (set *RangeSet) DeleteRange(low, high int64) {
	s := *set
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
					*set = s
				}
			} else {
				s[i].High = low
			}
		} else {
			if high < s[i].High {
				s[i].Low = high
			} else {
				s = append(s[:i], s[j:]...)
				*set = s
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

	s = append(s[:i], s[j:]...)
	*set = s
}

func (set RangeSet) Contains(single int64) bool {
	return set.ContainsRange(single, single+1)
}

func (set RangeSet) ContainsRange(low, high int64) bool {
	i := sort.Search(len(set), func(i int) bool { return set[i].High > low })
	return i < len(set) && set[i].Low <= low && high <= set[i].High && low < high
}

func (set RangeSet) ContainsAny(low, high int64) bool {
	i := sort.Search(len(set), func(i int) bool { return set[i].High > low })
	t := set[i:]
	j := i + sort.Search(len(t), func(i int) bool { return t[i].Low >= high })

	return i < j && low < high
}

func (set RangeSet) Equals(other RangeSet) bool {
	if len(set) != len(other) {
		return false
	}

	for i, r := range set {
		if r != other[i] {
			return false
		}
	}

	return true
}

func (set RangeSet) Union(other RangeSet) RangeSet {
	if len(set) < len(other) {
		set, other = other, set
	}

	if len(other) == 0 {
		// Always return a distinct RangeSet.
		result := make(RangeSet, len(set))
		copy(result, set)

		return result
	}

	var result RangeSet

	r := other[0]
	other = other[1:]

	for {
		i := sort.Search(len(set), func(i int) bool { return set[i].Low > r.Low })
		j := sort.Search(len(set), func(i int) bool { return set[i].High > r.High })

		if i > 0 && r.Low <= set[i-1].High {
			r.Low = set[i-1].Low
			i--
		}

		if j < len(set) && r.High >= set[j].Low {
			r.High = set[j].High
			j++
		}

		result = append(result, set[:i]...)

		if len(other) == 0 {
			result = append(result, r)
			result = append(result, set[j:]...)

			break
		}

		set, other = other, set[j:]
	}

	return result
}

func (set RangeSet) Intersect(other RangeSet) RangeSet {
	var result RangeSet

	for {
		if len(set) < len(other) {
			set, other = other, set
		}

		if len(other) == 0 {
			break
		}

		r := other[0]
		i := sort.Search(len(set), func(i int) bool { return set[i].High > r.Low })
		t := set[i:]
		j := i + sort.Search(len(t), func(i int) bool { return t[i].Low >= r.High })

		if i < j {
			start := len(result)
			result = append(result, set[i:j]...)

			if r0 := &result[start]; r0.Low < r.Low {
				r0.Low = r.Low
			}

			if r1 := &result[len(result)-1]; r1.High > r.High {
				r1.High = r.High
			}
			j--
		}

		set, other = set[j:], other[1:]
	}

	return result
}

func (set RangeSet) Complement() RangeSet {
	if len(set) == 0 {
		return RangeSet{{math.MinInt64, math.MaxInt64}}
	}

	var result RangeSet

	r0 := set[0]

	if r0.Low > math.MinInt64 {
		result = append(result, Range{math.MinInt64, r0.Low})
	}

	low := r0.High

	for _, r := range set[1:] {
		result = append(result, Range{low, r.Low})
		low = r.High
	}

	if low < math.MaxInt64 {
		result = append(result, Range{low, math.MaxInt64})
	}

	return result
}

func (set RangeSet) Sum() uint64 {
	acc := uint64(0)
	for _, r := range set {
		acc += uint64(r.High - r.Low)
	}

	return acc
}
