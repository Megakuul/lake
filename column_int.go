package lake

import (
	"github.com/megakuul/lake/internal/catalog"
	"github.com/parquet-go/parquet-go"
)

var (
	_ filterable   = Int{}
	_ aggregatable = Int{}
	_ boundable    = Int{}
	_ groupable    = Int{}
)

type Int struct {
	filters    []Filter[int64]
	aggregator Aggregator[int64]
	grouper    Grouper
	Data       int64 `parquet:"data"`
}

func NewInt[T ~int | ~int64](value T) Int {
	return Int{Data: int64(value)}
}

func AggrInt(aggregation Aggregator[int64]) Int {
	return Int{aggregator: aggregation}
}

func GroupInt(grouper Grouper) Int {
	return Int{grouper: grouper}
}

func FilterInt(filters ...Filter[int64]) Int {
	return Int{filters: filters}
}

func (i Int) createRange() catalog.Range {
	var max, min int64
	var maxEnabled, minEnabled bool
	for _, filter := range i.filters {
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
		Kind:       catalog.ColumnInt,
		MaxEnabled: maxEnabled,
		MinEnabled: minEnabled,
		MaxInt:     max,
		MinInt:     min,
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
		if op.check != nil && !op.check(v.Int64()) {
			return false
		}
	}
	return true
}

func (i Int) canGroup() bool {
	return i.grouper != nil
}

func (i Int) group(value parquet.Value) parquet.Value {
	return i.grouper(value)
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
