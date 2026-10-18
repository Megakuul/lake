package lake

import (
	"slices"
	"time"
)

type Filter[T ~float64 | ~int64 | ~string] struct {
	max, min *T
	check    func(T) bool
}

// Eq checks for an exact match on the property.
func Eq[T ~float64 | ~int64 | ~string](operand T) Filter[T] {
	return Filter[T]{
		max: &operand,
		min: &operand,
		check: func(left T) bool {
			return left == operand
		},
	}
}

// In checks if one of the provided operands matches the row value.
func In[T ~float64 | ~int64 | ~string](operands ...T) Filter[T] {
	f := Filter[T]{
		check: func(left T) bool {
			return slices.Contains(operands, left)
		},
	}
	if len(operands) > 0 {
		f.max, f.min = new(slices.Max(operands)), new(slices.Min(operands))
	}
	return f
}

// Before only works for time.UnixNano (nanoseconds) data, it checks if the row value is BEFORE the operand.
func Before(operand time.Time) Filter[int64] {
	return Filter[int64]{
		max: new(operand.UnixNano()),
		min: nil,
		check: func(left int64) bool {
			return time.Unix(0, left).Before(operand)
		},
	}
}

// After only works for time.UnixNano (nanoseconds) data, it checks if the row value is AFTER the operand.
func After(operand time.Time) Filter[int64] {
	return Filter[int64]{
		max: nil,
		min: new(operand.UnixNano()),
		check: func(left int64) bool {
			return time.Unix(0, left).After(operand)
		},
	}
}

// Gte checks if the row value is greater or equal than the operand.
func Gte[T ~float64 | ~int64 | ~string](operand T) Filter[T] {
	return Filter[T]{
		max: nil,
		min: &operand,
		check: func(left T) bool {
			return left >= operand
		},
	}
}

// Gt checks if the row value is greater than the operand.
func Gt[T ~float64 | ~int64 | ~string](operand T) Filter[T] {
	return Filter[T]{
		max: nil,
		min: &operand,
		check: func(left T) bool {
			return left > operand
		},
	}
}

// Lte checks if the row value is less or equal than the operand.
func Lte[T ~float64 | ~int64 | ~string](operand T) Filter[T] {
	return Filter[T]{
		max: &operand,
		min: nil,
		check: func(left T) bool {
			return left <= operand
		},
	}
}

// Lt checks if the row value is less than the operand.
func Lt[T ~float64 | ~int64 | ~string](operand T) Filter[T] {
	return Filter[T]{
		max: &operand,
		min: nil,
		check: func(left T) bool {
			return left < operand
		},
	}
}
