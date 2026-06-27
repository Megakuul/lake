package lakedb

import (
	"iter"

	"github.com/parquet-go/parquet-go"
)

var (
	_ genericFilter     = &Double{}
	_ rangeFilter       = &Double{}
	_ genericAggregator = &Double{}
)

type Double struct {
	doubleOperation
	Data float64 `parquet:"data"`
}

func NewDouble(value float64) Double {
	return Double{Data: value}
}

type doubleOperation struct {
	maxValue, minValue any
	filterOps          []func(float64) bool
	aggregator         func(iter.Seq[float64], int) float64
}

func (d doubleOperation) max() any {
	return d.maxValue
}

func (d doubleOperation) min() any {
	return d.minValue
}

func (d doubleOperation) filter(v parquet.Value) bool {
	if v.Kind() != parquet.Double {
		return true
	}
	for _, op := range d.filterOps {
		if !op(v.Double()) {
			return false
		}
	}
	return true
}

func (d doubleOperation) aggregate(rows []parquet.Value) parquet.Value {
	return parquet.DoubleValue(d.aggregator(func(yield func(float64) bool) {
		for _, row := range rows {
			if !yield(row.Double()) {
				return
			}
		}
	}, len(rows)))
}

func NewDoubleOperation() *doubleOperation {
	return new(doubleOperation)
}

// filters
func (d *doubleOperation) Gte(operand float64) *doubleOperation {
	d.minValue = operand
	d.filterOps = append(d.filterOps, func(i float64) bool {
		return i >= operand
	})
	return d
}

func (d *doubleOperation) Lte(operand float64) *doubleOperation {
	d.maxValue = operand
	d.filterOps = append(d.filterOps, func(i float64) bool {
		return i <= operand
	})
	return d
}

func (d *doubleOperation) Gt(operand float64) *doubleOperation {
	d.minValue = operand
	d.filterOps = append(d.filterOps, func(i float64) bool {
		return i > operand
	})
	return d
}

func (d *doubleOperation) Lt(operand float64) *doubleOperation {
	d.maxValue = operand
	d.filterOps = append(d.filterOps, func(i float64) bool {
		return i < operand
	})
	return d
}

// aggregators

func (d *doubleOperation) Sum() *doubleOperation {
	d.aggregator = func(rows iter.Seq[float64], _ int) (result float64) {
		for row := range rows {
			result += row
		}
		return result
	}
	return d
}

func (d *doubleOperation) Count() *doubleOperation {
	d.aggregator = func(_ iter.Seq[float64], count int) float64 {
		return float64(count)
	}
	return d
}

func (d *doubleOperation) Min() *doubleOperation {
	d.aggregator = func(rows iter.Seq[float64], _ int) (result float64) {
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

func (d *doubleOperation) Max() *doubleOperation {
	d.aggregator = func(rows iter.Seq[float64], _ int) (result float64) {
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

func (d *doubleOperation) Avg() *doubleOperation {
	d.aggregator = func(rows iter.Seq[float64], count int) float64 {
		var total float64
		for row := range rows {
			total += row
		}
		return total / float64(count)
	}
	return d
}

func (d *doubleOperation) End() Double {
	return Double{*d, 0}
}
