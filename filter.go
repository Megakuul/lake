package lake

import (
	"slices"
	"time"
)

type Filter[T int64 | float64 | string] struct {
	max, min *T
	check    func(T) bool
}

// Eq checks for an exact match on the property.
func Eq[T float64 | int64 | string](operand T) Filter[T] {
	return Filter[T]{
		check: func(left T) bool {
			return left == operand
		},
	}
}

// In checks if one of the provided operands matches the row value.
func In[T float64 | int64 | string](operands ...T) Filter[T] {
	return Filter[T]{
		check: func(left T) bool {
			return slices.Contains(operands, left)
		},
	}
}

// Before only works for time.Unix (seconds) data, it checks if operand is BEFORE the row value.
func Before(operand time.Time) Filter[int64] {
	return Filter[int64]{
		max: new(operand.Unix()),
		min: nil,
		check: func(left int64) bool {
			return time.Unix(left, 0).Before(operand)
		},
	}
}

// After only works for time.Unix (seconds) data, it checks if operand is AFTER the row value.
func After(operand time.Time) Filter[int64] {
	return Filter[int64]{
		max: nil,
		min: new(operand.Unix()),
		check: func(left int64) bool {
			return time.Unix(left, 0).After(operand)
		},
	}
}

// Gte checks if the operand is greater or equals than the row value.
func Gte[T float64 | int64 | string](operand T) Filter[T] {
	return Filter[T]{
		max: nil,
		min: &operand,
		check: func(left T) bool {
			return left >= operand
		},
	}
}

// Gt checks if the operand is greater than the row value.
func Gt[T float64 | int64 | string](operand T) Filter[T] {
	return Filter[T]{
		max: nil,
		min: &operand,
		check: func(left T) bool {
			return left > operand
		},
	}
}

// Lte checks if the operand is less or equals than the row value.
func Lte[T float64 | int64 | string](operand T) Filter[T] {
	return Filter[T]{
		max: nil,
		min: &operand,
		check: func(left T) bool {
			return left <= operand
		},
	}
}

// Lt checks if the operand is less than the row value.
func Lt[T float64 | int64 | string](operand T) Filter[T] {
	return Filter[T]{
		max: nil,
		min: &operand,
		check: func(left T) bool {
			return left < operand
		},
	}
}
