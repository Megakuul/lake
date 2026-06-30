package lake

import (
	"github.com/parquet-go/parquet-go"
)

type Grouper func(parquet.Value) (string, parquet.Value)

func Exact(value parquet.Value) (string, parquet.Value) {
	return value.String(), value
}
