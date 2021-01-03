package main

import (
	"math"
	"testing"
)

func TestRangeSet(t *testing.T) {
	equals := func(a, b RangeSet) bool { return a.Equals(b) }

	t.Run("TestAdd", func(t *testing.T) {
		addRange := func(s RangeSet, r Range) RangeSet {
			s.AddRange(r.Low, r.High)
			return s
		}
		addSingle := func(s RangeSet, single int64) RangeSet {
			s.Add(single)
			return s
		}
		assert(t, "Case 1", equals(
			addRange(RangeSet{{1, 4}, {9, 12}}, Range{5, 8}),
			RangeSet{{1, 4}, {5, 8}, {9, 12}},
		))
		assert(t, "Case 2", equals(
			addSingle(RangeSet{{1, 4}, {9, 12}}, 6),
			RangeSet{{1, 4}, {6, 7}, {9, 12}},
		))
		assert(t, "Case 3", equals(
			addRange(RangeSet{{1, 4}, {9, 12}}, Range{4, 8}),
			RangeSet{{1, 8}, {9, 12}},
		))
		assert(t, "Case 4", equals(
			addRange(RangeSet{{1, 4}, {9, 12}}, Range{5, 9}),
			RangeSet{{1, 4}, {5, 12}},
		))
		assert(t, "Case 5", equals(
			addRange(RangeSet{{1, 4}, {9, 12}}, Range{4, 9}),
			RangeSet{{1, 12}},
		))
		assert(t, "Case 6", equals(
			addSingle(RangeSet{{1, 4}, {9, 12}}, 10),
			RangeSet{{1, 4}, {9, 12}},
		))
		assert(t, "Case 7", equals(
			addRange(RangeSet{{1, 4}, {9, 12}}, Range{9, 12}),
			RangeSet{{1, 4}, {9, 12}},
		))
		assert(t, "Case 8", equals(
			addRange(RangeSet{{1, 4}, {9, 12}}, Range{12, 9}),
			RangeSet{{1, 4}, {9, 12}},
		))
	})
	t.Run("TestDelete", func(t *testing.T) {
		deleteRange := func(s RangeSet, r Range) RangeSet {
			s.DeleteRange(r.Low, r.High)
			return s
		}
		deleteSingle := func(s RangeSet, single int64) RangeSet {
			s.Delete(single)
			return s
		}
		assert(t, "Case 1", equals(
			deleteRange(RangeSet{{1, 4}, {7, 10}, {13, 16}}, Range{7, 10}),
			RangeSet{{1, 4}, {13, 16}},
		))
		assert(t, "Case 2", equals(
			deleteRange(RangeSet{{1, 4}, {7, 10}, {13, 16}}, Range{7, 9}),
			RangeSet{{1, 4}, {9, 10}, {13, 16}},
		))
		assert(t, "Case 3", equals(
			deleteRange(RangeSet{{1, 4}, {7, 10}, {13, 16}}, Range{8, 10}),
			RangeSet{{1, 4}, {7, 8}, {13, 16}},
		))
		assert(t, "Case 4", equals(
			deleteSingle(RangeSet{{1, 4}, {7, 10}, {13, 16}}, 8),
			RangeSet{{1, 4}, {7, 8}, {9, 10}, {13, 16}},
		))
		assert(t, "Case 5", equals(
			deleteRange(RangeSet{{1, 4}, {7, 10}, {13, 16}}, Range{1, 16}),
			RangeSet{},
		))
		assert(t, "Case 6", equals(
			deleteRange(RangeSet{{1, 4}, {7, 10}, {13, 16}}, Range{1, 15}),
			RangeSet{{15, 16}},
		))
		assert(t, "Case 7", equals(
			deleteRange(RangeSet{{1, 4}, {7, 10}, {13, 16}}, Range{2, 16}),
			RangeSet{{1, 2}},
		))
		assert(t, "Case 8", equals(
			deleteSingle(RangeSet{{1, 4}, {7, 10}, {13, 16}}, 5),
			RangeSet{{1, 4}, {7, 10}, {13, 16}},
		))
		assert(t, "Case 9", equals(
			deleteRange(RangeSet{{1, 4}, {7, 10}, {13, 16}}, Range{4, 7}),
			RangeSet{{1, 4}, {7, 10}, {13, 16}},
		))
		assert(t, "Case 10", equals(
			deleteRange(RangeSet{{1, 4}, {7, 10}, {13, 16}}, Range{7, 4}),
			RangeSet{{1, 4}, {7, 10}, {13, 16}},
		))
	})
	t.Run("TestContains", func(t *testing.T) {
		s := RangeSet{{1, 3}, {5, 7}}
		assert(t, "Case 1", s.Contains(0) == false)
		assert(t, "Case 2", s.Contains(1) == true)
		assert(t, "Case 3", s.Contains(2) == true)
		assert(t, "Case 4", s.Contains(3) == false)
		assert(t, "Case 5", s.Contains(4) == false)
		assert(t, "Case 6", s.Contains(5) == true)
		assert(t, "Case 7", s.Contains(6) == true)
		assert(t, "Case 8", s.Contains(7) == false)
		assert(t, "Case 9", s.ContainsRange(1, 3) == true)
		assert(t, "Case 10", s.ContainsRange(3, 5) == false)
		assert(t, "Case 11", s.ContainsRange(5, 7) == true)
		assert(t, "Case 12", s.ContainsRange(1, 7) == false)
		assert(t, "Case 13", s.ContainsRange(1, 1) == false)
		assert(t, "Case 14", s.ContainsRange(2, 2) == false)
		assert(t, "Case 15", s.ContainsAny(1, 3) == true)
		assert(t, "Case 16", s.ContainsAny(3, 5) == false)
		assert(t, "Case 17", s.ContainsAny(5, 7) == true)
		assert(t, "Case 18", s.ContainsAny(1, 7) == true)
		assert(t, "Case 19", s.ContainsAny(1, 1) == false)
		assert(t, "Case 20", s.ContainsAny(2, 2) == false)
	})
	t.Run("TestEquals", func(t *testing.T) {
		assert(t, "Case 1", equals(
			RangeSet{{1, 3}, {5, 7}},
			RangeSet{{1, 3}, {5, 7}},
		))
		assert(t, "Case 2", !equals(
			RangeSet{{1, 3}, {5, 7}},
			RangeSet{{1, 3}, {5, 9}},
		))
		assert(t, "Case 3", !equals(
			RangeSet{{1, 3}, {5, 7}},
			RangeSet{{1, 3}},
		))
	})
	t.Run("TestUnion", func(t *testing.T) {
		assert(t, "Case 1", equals(
			RangeSet{{1, 3}, {5, 7}}.Union(RangeSet{}),
			RangeSet{{1, 3}, {5, 7}},
		))
		assert(t, "Case 2", equals(
			RangeSet{}.Union(RangeSet{{1, 3}, {5, 7}}),
			RangeSet{{1, 3}, {5, 7}},
		))
		assert(t, "Case 3", equals(
			RangeSet{{1, 3}}.Union(RangeSet{{5, 7}}),
			RangeSet{{1, 3}, {5, 7}},
		))
		assert(t, "Case 4", equals(
			RangeSet{{1, 5}}.Union(RangeSet{{3, 7}}),
			RangeSet{{1, 7}},
		))
		assert(t, "Case 5", equals(
			RangeSet{{2, 6}, {7, 12}}.Union(RangeSet{{1, 4}, {5, 9}, {10, 16}}),
			RangeSet{{1, 16}},
		))
	})
	t.Run("TestIntersect", func(t *testing.T) {
		assert(t, "Case 1", equals(
			RangeSet{{1, 3}}.Intersect(RangeSet{{5, 7}}),
			RangeSet{},
		))
		assert(t, "Case 2", equals(
			RangeSet{{1, 5}}.Intersect(RangeSet{{3, 7}}),
			RangeSet{{3, 5}},
		))
		assert(t, "Case 3", equals(
			RangeSet{{2, 6}, {7, 12}}.Intersect(RangeSet{{1, 4}, {5, 9}, {10, 16}}),
			RangeSet{{2, 4}, {5, 6}, {7, 9}, {10, 12}},
		))
	})
	t.Run("TestComplement", func(t *testing.T) {
		assert(t, "Case 1", equals(
			RangeSet{}.Complement(),
			RangeSet{{math.MinInt64, math.MaxInt64}},
		))
		assert(t, "Case 2", equals(
			RangeSet{{math.MinInt64, math.MaxInt64}}.Complement(),
			RangeSet{},
		))
		assert(t, "Case 3", equals(
			RangeSet{{1, 4}, {6, 9}}.Complement(),
			RangeSet{{math.MinInt64, 1}, {4, 6}, {9, math.MaxInt64}},
		))
	})
	t.Run("TestSum", func(t *testing.T) {
		assert(t, "Case 1", RangeSet{}.Sum() == 0)
		assert(t, "Case 2", RangeSet{{1, 4}}.Sum() == 3)
		assert(t, "Case 3", RangeSet{{1, 3}, {5, 7}}.Sum() == 4)
	})
}

func assert(t *testing.T, message string, ok bool) {
	if !ok {
		t.Fatal(message)
	}
}
