package lakedb

import "strings"

type Int struct {
	Data   int64     `parquet:"data"`
	filter intFilter `parquet:"-"`
}

func NewInt(value int64) Int {
	return Int{Data: value}
}

type intFilter struct {
	max       *int64
	min       *int64
	filterOps []func(int64) bool
}

func IntFilter() *intFilter {
	return new(intFilter)
}

func (f *intFilter) Gte(operand int64) *intFilter {
	f.min = &operand
	f.filterOps = append(f.filterOps, func(i int64) bool {
		return i >= operand
	})
	return f
}

func (f *intFilter) Lte(operand int64) *intFilter {
	f.max = &operand
	f.filterOps = append(f.filterOps, func(i int64) bool {
		return i <= operand
	})
	return f
}

func (f *intFilter) Gt(operand int64) *intFilter {
	f.min = &operand
	f.filterOps = append(f.filterOps, func(i int64) bool {
		return i > operand
	})
	return f
}

func (f *intFilter) Lt(operand int64) *intFilter {
	f.max = &operand
	f.filterOps = append(f.filterOps, func(i int64) bool {
		return i < operand
	})
	return f
}

func (f *intFilter) End() Int {
	return Int{
		filter: *f,
	}
}

type Double struct {
	Data   float64      `parquet:"data"`
	filter doubleFilter `parquet:"-"`
}

func NewDouble(value float64) Double {
	return Double{Data: value}
}

type doubleFilter struct {
	max       *float64
	min       *float64
	filterOps []func(float64) bool
}

func DoubleFilter() *doubleFilter {
	return new(doubleFilter)
}

func (f *doubleFilter) Gte(operand float64) *doubleFilter {
	f.min = &operand
	f.filterOps = append(f.filterOps, func(i float64) bool {
		return i >= operand
	})
	return f
}

func (f *doubleFilter) Lte(operand float64) *doubleFilter {
	f.max = &operand
	f.filterOps = append(f.filterOps, func(i float64) bool {
		return i <= operand
	})
	return f
}

func (f *doubleFilter) Gt(operand float64) *doubleFilter {
	f.min = &operand
	f.filterOps = append(f.filterOps, func(i float64) bool {
		return i > operand
	})
	return f
}

func (f *doubleFilter) Lt(operand float64) *doubleFilter {
	f.max = &operand
	f.filterOps = append(f.filterOps, func(i float64) bool {
		return i < operand
	})
	return f
}

func (f *doubleFilter) End() Double {
	return Double{
		filter: *f,
	}
}

type String struct {
	Data   string       `parquet:"data"`
	filter stringFilter `parquet:"-"`
}

func NewString(value string) String {
	return String{Data: value}
}

type stringFilter struct {
	filterOps []func(string) bool
}

func StringFilter() *stringFilter {
	return new(stringFilter)
}

func (f *stringFilter) Eq(operand string) *stringFilter {
	f.filterOps = append(f.filterOps, func(v string) bool {
		return v == operand
	})
	return f
}

func (f *stringFilter) Ne(operand string) *stringFilter {
	f.filterOps = append(f.filterOps, func(v string) bool {
		return v != operand
	})
	return f
}

func (f *stringFilter) Contains(operand string) *stringFilter {
	f.filterOps = append(f.filterOps, func(v string) bool {
		return strings.Contains(v, operand)
	})
	return f
}

func (f *stringFilter) End() String {
	return String{
		filter: *f,
	}
}
