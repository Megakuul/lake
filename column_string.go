package lake

import (
	"github.com/megakuul/lake/internal/catalog"
	"github.com/parquet-go/parquet-go"
)

var (
	_ filterable = String{}
	_ boundable  = String{}
	_ groupable  = String{}
)

type String struct {
	filters []Filter[string]
	grouper Grouper
	Data    string `parquet:"data"`
}

func NewString[T ~string](value T) String {
	return String{Data: string(value)}
}

func GroupString(grouper Grouper) String {
	return String{grouper: grouper}
}

func FilterString(filters ...Filter[string]) String {
	f := String{
		filters: filters,
	}
	return f
}

func (s String) createRange() catalog.Range {
	var max, min string
	var maxEnabled, minEnabled bool
	for _, filter := range s.filters {
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
		Kind:       catalog.ColumnString,
		MaxEnabled: maxEnabled,
		MinEnabled: minEnabled,
		MaxString:  max,
		MinString:  min,
	}
}

func (s String) canGroup() bool {
	return s.grouper != nil
}

func (s String) group(value parquet.Value) parquet.Value {
	return s.grouper(value)
}

func (s String) canFilter() bool {
	return len(s.filters) != 0
}

func (s String) filter(v parquet.Value) bool {
	if v.Kind() != parquet.ByteArray {
		return true
	}
	for _, op := range s.filters {
		if op.check != nil && !op.check(string(v.ByteArray())) {
			return false
		}
	}
	return true
}
