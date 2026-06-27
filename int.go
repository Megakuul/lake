package lakedb

import (
	"iter"

	"github.com/parquet-go/parquet-go"
)

var (
	_ genericFilter     = &Int{}
	_ rangeFilter       = &Int{}
	_ genericAggregator = &Int{}
)

type Int struct {
	intOperation
	Data int64 `parquet:"data"`
}

func NewInt(value int64) Int {
	return Int{Data: value}
}

type intOperation struct {
	maxValue, minValue any
	filterOps          []func(int64) bool
	aggregator         func(iter.Seq[int64], int) int64
}

func (d intOperation) max() any {
	return d.maxValue
}

func (d intOperation) min() any {
	return d.minValue
}

func (d intOperation) filter(v parquet.Value) bool {
	if v.Kind() != parquet.Int64 {
		return true
	}
	for _, op := range d.filterOps {
		if !op(v.Int64()) {
			return false
		}
	}
	return true
}

func (d intOperation) aggregate(rows []parquet.Value) parquet.Value {
	if d.aggregator == nil {
		return parquet.NullValue() // if aggregation function is unset we just set default value
	}
	return parquet.Int64Value(d.aggregator(func(yield func(int64) bool) {
		for _, row := range rows {
			if !yield(row.Int64()) {
				return
			}
		}
	}, len(rows)))
}

func NewIntOp() *intOperation {
	return &intOperation{}
}

// filters
func (d *intOperation) Gte(operand int64) *intOperation {
	d.minValue = operand
	d.filterOps = append(d.filterOps, func(i int64) bool {
		return i >= operand
	})
	return d
}

func (d *intOperation) Lte(operand int64) *intOperation {
	d.maxValue = operand
	d.filterOps = append(d.filterOps, func(i int64) bool {
		return i <= operand
	})
	return d
}

func (d *intOperation) Gt(operand int64) *intOperation {
	d.minValue = operand
	d.filterOps = append(d.filterOps, func(i int64) bool {
		return i > operand
	})
	return d
}

func (d *intOperation) Lt(operand int64) *intOperation {
	d.maxValue = operand
	d.filterOps = append(d.filterOps, func(i int64) bool {
		return i < operand
	})
	return d
}

// aggregators

func (d *intOperation) Sum() *intOperation {
	d.aggregator = func(rows iter.Seq[int64], _ int) (result int64) {
		for row := range rows {
			result += row
		}
		return result
	}
	return d
}

func (d *intOperation) Count() *intOperation {
	d.aggregator = func(_ iter.Seq[int64], count int) int64 {
		return int64(count)
	}
	return d
}

func (d *intOperation) Min() *intOperation {
	d.aggregator = func(rows iter.Seq[int64], _ int) (result int64) {
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
	return d
}

func (d *intOperation) Max() *intOperation {
	d.aggregator = func(rows iter.Seq[int64], _ int) (result int64) {
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
	return d
}

func (d *intOperation) Avg() *intOperation {
	d.aggregator = func(rows iter.Seq[int64], count int) int64 {
		var total int64
		for row := range rows {
			total += row
		}
		return total / int64(count)
	}
	return d
}

func (d *intOperation) End() Int {
	return Int{*d, 0}
}
