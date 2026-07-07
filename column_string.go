package lake

import (
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

func NewString(value string) String {
	return String{Data: value}
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

func (s String) higher(than any) (any, bool) {
	if than, ok := than.(string); ok {
		return &s.Data, s.Data > than
	}
	return nil, false
}

func (s String) lower(than any) (any, bool) {
	if than, ok := than.(string); ok {
		return &s.Data, s.Data < than
	}
	return nil, false
}

func (s String) max() any {
	var max *string
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

func (s String) min() any {
	var min *string
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
		if !op.check(string(v.ByteArray())) {
			return false
		}
	}
	return true
}
