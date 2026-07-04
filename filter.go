package lake

import (
	"slices"
	"time"
)

type Filter[T int64 | float64 | string] struct {
	max, min *T
	check    func(T) bool
}

func Eq[T float64 | int64 | string](operand T) Filter[T] {
	return Filter[T]{
		check: func(left T) bool {
			return left == operand
		},
	}
}

func In[T float64 | int64 | string](operands ...T) Filter[T] {
	return Filter[T]{
		check: func(left T) bool {
			return slices.Contains(operands, left)
		},
	}
}

func Before(operand time.Time) Filter[int64] {
	return Filter[int64]{
		max: new(operand.Unix()),
		min: nil,
		check: func(left int64) bool {
			return time.Unix(left, 0).Before(operand)
		},
	}
}

func After(operand time.Time) Filter[int64] {
	return Filter[int64]{
		max: nil,
		min: new(operand.Unix()),
		check: func(left int64) bool {
			return time.Unix(left, 0).After(operand)
		},
	}
}

func Gte[T float64 | int64 | string](operand T) Filter[T] {
	return Filter[T]{
		max: nil,
		min: &operand,
		check: func(left T) bool {
			return left >= operand
		},
	}
}

func Gt[T float64 | int64 | string](operand T) Filter[T] {
	return Filter[T]{
		max: nil,
		min: &operand,
		check: func(left T) bool {
			return left > operand
		},
	}
}

func Lte[T float64 | int64 | string](operand T) Filter[T] {
	return Filter[T]{
		max: nil,
		min: &operand,
		check: func(left T) bool {
			return left <= operand
		},
	}
}

func Lt[T float64 | int64 | string](operand T) Filter[T] {
	return Filter[T]{
		max: nil,
		min: &operand,
		check: func(left T) bool {
			return left < operand
		},
	}
}
