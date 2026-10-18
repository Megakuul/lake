package lake

import (
	"github.com/megakuul/lake/internal/catalog"
	"github.com/parquet-go/parquet-go"
)

var (
	_ filterable   = Float{}
	_ aggregatable = Float{}
	_ boundable    = Float{}
	_ groupable    = Float{}
)

type Float struct {
	filters    []Filter[float64]
	grouper    Grouper
	aggregator Aggregator[float64]
	Data       float64 `parquet:"data"`
}

func NewFloat[T ~float64](value T) Float {
	return Float{Data: float64(value)}
}

func GroupFloat(grouper Grouper) Float {
	return Float{grouper: grouper}
}

func AggrFloat(aggregation Aggregator[float64]) Float {
	return Float{aggregator: aggregation}
}

func FilterFloat(filters ...Filter[float64]) Float {
	return Float{filters: filters}
}

func (f Float) createRange() catalog.Range {
	var max, min float64
	var maxEnabled, minEnabled bool
	for _, filter := range f.filters {
		if filter.max != nil && (!maxEnabled || *filter.max < max) {
			maxEnabled = true
			max = *filter.max
		}
		if filter.min != nil && (!minEnabled || *filter.min > min) {
			minEnabled = true
			min = *filter.min
		}
	}
	return catalog.Range{
		Kind:       catalog.ColumnFloat,
		MaxEnabled: maxEnabled,
		MinEnabled: minEnabled,
		MaxFloat:   max,
		MinFloat:   min,
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
		if op.check != nil && !op.check(v.Double()) {
			return false
		}
	}
	return true
}

func (f Float) canGroup() bool {
	return f.grouper != nil
}

func (f Float) group(value parquet.Value) parquet.Value {
	return f.grouper(value)
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
