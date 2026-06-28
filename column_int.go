package lake

import (
	"github.com/parquet-go/parquet-go"
)

var (
	_ filterable   = Int{}
	_ aggregatable = Int{}
	_ boundable    = Int{}
)

type Int struct {
	filters    []Filter[int64]
	aggregator Aggregator[int64]
	Data       int64 `parquet:"data"`
}

func NewInt(value int64) Int {
	return Int{Data: value}
}

func AggrInt(aggregation Aggregator[int64], grouping ...Filter[int64]) Int {
	f := Int{
		aggregator: aggregation,
		filters:    grouping,
	}
	return f
}

func FilterInt(filters ...Filter[int64]) Int {
	f := Int{
		filters: filters,
	}
	return f
}

func (i Int) higher(than any) (any, bool) {
	if than, ok := than.(int64); ok {
		return &i.Data, i.Data > than
	}
	return nil, false
}

func (i Int) lower(than any) (any, bool) {
	if than, ok := than.(int64); ok {
		return &i.Data, i.Data < than
	}
	return nil, false
}

func (i Int) max() any {
	var max *int64
	for _, filter := range i.filters {
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

func (i Int) min() any {
	var min *int64
	for _, filter := range i.filters {
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

func (i Int) canFilter() bool {
	return len(i.filters) != 0
}

func (i Int) filter(v parquet.Value) bool {
	if v.Kind() != parquet.Int64 {
		return true
	}
	for _, op := range i.filters {
		if !op.check(v.Int64()) {
			return false
		}
	}
	return true
}

func (i Int) canAggregate() bool {
	return i.aggregator != nil
}

func (i Int) aggregate(rows []parquet.Value) parquet.Value {
	return parquet.Int64Value(i.aggregator(func(yield func(int64) bool) {
		for _, row := range rows {
			if !yield(row.Int64()) {
				return
			}
		}
	}, len(rows)))
}
