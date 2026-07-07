package lake

import (
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

func NewFloat(value float64) Float {
	return Float{Data: value}
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

func (s Float) higher(than any) (any, bool) {
	if than, ok := than.(float64); ok {
		return &s.Data, s.Data > than
	}
	return nil, false
}

func (s Float) lower(than any) (any, bool) {
	if than, ok := than.(float64); ok {
		return &s.Data, s.Data < than
	}
	return nil, false
}

func (s Float) max() any {
	var max *float64
	for _, filter := range s.filters {
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

func (s Float) min() any {
	var min *float64
	for _, filter := range s.filters {
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

func (s Float) canFilter() bool {
	return len(s.filters) != 0
}

func (s Float) filter(v parquet.Value) bool {
	if v.Kind() != parquet.Double {
		return true
	}
	for _, op := range s.filters {
		if !op.check(v.Double()) {
			return false
		}
	}
	return true
}

func (s Float) canGroup() bool {
	return s.grouper != nil
}

func (s Float) group(value parquet.Value) parquet.Value {
	return s.grouper(value)
}

func (s Float) canAggregate() bool {
	return s.aggregator != nil
}

func (s Float) aggregate(rows []parquet.Value) parquet.Value {
	if s.aggregator == nil {
		return parquet.NullValue()
	}
	return parquet.DoubleValue(s.aggregator(func(yield func(float64) bool) {
		for _, row := range rows {
			if !yield(row.Double()) {
				return
			}
		}
	}, len(rows)))
}
