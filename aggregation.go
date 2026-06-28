package lake

import "iter"

type Aggregator[T int64 | float64] func(iter.Seq[T], int) T

func Sum[T int64 | float64](rows iter.Seq[T], _ int) (result T) {
	for row := range rows {
		result += row
	}
	return result
}

func Count[T int64 | float64](_ iter.Seq[T], count int) (result T) {
	return T(count)
}

func Min[T int64 | float64](rows iter.Seq[T], count int) (result T) {
	active := false
	for row := range rows {
		if !active {
			result = row
			active = true
		} else if row < result {
			result = row
		}
	}
	return result
}

func Max[T int64 | float64](rows iter.Seq[T], count int) (result T) {
	active := false
	for row := range rows {
		if !active {
			result = row
			active = true
		} else if row > result {
			result = row
		}
	}
	return result
}

func Avg[T int64 | float64](rows iter.Seq[T], count int) (result T) {
	var total T
	for row := range rows {
		total += row
	}
	return total / T(count)
}
