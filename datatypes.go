package lakedb

func Filter[T DoubleFilter | IntFilter]() *T {
	return new(T)
}

type Double struct {
	data      float64              `parquet:"data"`
	filterMax *float64             `parquet:"-"`
	filterMin *float64             `parquet:"-"`
	filterOps []func(float64) bool `parquet:"-"`
}

func DoubleValue(value float64) Double {
	return Double{data: value}
}

type DoubleFilter Double

func (f *DoubleFilter) Gt(operand float64) *DoubleFilter {
	f.filterMin = &operand
	return f
}

func (f *DoubleFilter) Lt(operand float64) *DoubleFilter {
	f.filterMax = &operand
	return f
}

func (f *Double) Check(operand float64) bool {
	for _, op := range f.filterOps {
		if !op(operand) {
			return false
		}
	}
	return true
}

type Int struct {
	data      int64              `parquet:"data"`
	filterMax *int64             `parquet:"-"`
	filterMin *int64             `parquet:"-"`
	filterOps []func(int64) bool `parquet:"-"`
}

func IntValue(value int64) Int {
	return Int{data: value}
}

type IntFilter Int

func (f *IntFilter) Gt(operand int64) *IntFilter {
	f.filterMin = &operand
	return f
}

func (f *IntFilter) Lt(operand int64) *IntFilter {
	f.filterMax = &operand
	return f
}
