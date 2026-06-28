package lake

import (
	"github.com/parquet-go/parquet-go"
)

var (
	_ filterable   = Float{}
	_ aggregatable = Float{}
	_ boundable    = Float{}
)

type Float struct {
	filters    []Filter[float64]
	aggregator Aggregator[float64]
	Data       float64 `parquet:"data"`
}

func NewFloat(value float64) Float {
	return Float{Data: value}
}

func AggrFloat(aggregation Aggregator[float64], grouping ...Filter[float64]) Float {
	f := Float{
		aggregator: aggregation,
		filters:    grouping,
	}
	return f
}

func FilterFloat(filters ...Filter[float64]) Float {
	f := Float{
		filters: filters,
	}
	return f
}

func (f Float) higher(than any) (any, bool) {
	if than, ok := than.(float64); ok {
		return &f.Data, f.Data > than
	}
	return nil, false
}

func (f Float) lower(than any) (any, bool) {
	if than, ok := than.(float64); ok {
		return &f.Data, f.Data < than
	}
	return nil, false
}

func (f Float) max() any {
	var max *float64
	for _, filter := range f.filters {
		if filter.max != nil && (max == nil || *max < *filter.max) {
			max = filter.max
		}
	}
	if max == nil {
		return nil
	} else {
		return *max
	}
}

func (f Float) min() any {
	var min *float64
	for _, filter := range f.filters {
		if filter.min != nil && (min == nil || *min > *filter.min) {
			min = filter.min
		}
	}
	if min == nil {
		return nil
	} else {
		return *min
	}
}

func (f Float) canFilter() bool {
	return len(f.filters) != 0
}

func (f Float) filter(v parquet.Value) bool {
	if v.Kind() != parquet.Double {
		return true
	}
	for _, op := range f.filters {
		if !op.check(v.Double()) {
			return false
		}
	}
	return true
}

func (f Float) canAggregate() bool {
	return f.aggregator != nil
}

func (f Float) aggregate(rows []parquet.Value) parquet.Value {
	if f.aggregator == nil {
		return parquet.NullValue()
	}
	return parquet.DoubleValue(f.aggregator(func(yield func(float64) bool) {
		for _, row := range rows {
			if !yield(row.Double()) {
				return
			}
		}
	}, len(rows)))
}
