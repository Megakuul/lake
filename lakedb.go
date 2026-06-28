package lake

import (
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// Table is the interface that must be implemented by all parquet table structs.
type Table interface {
	// Name defines the table name in parquet. It is a hard contract to the data.
	Name() string
	// Sorting defines the table column sorting.
	Sorting() parquet.SortingOption
}

// getColumnName extracts the parquet column name from a struct field.
// It is compatible to the parquet library struct tagging system.
func getColumnName(field reflect.StructField) string {
	tag := strings.SplitN(field.Tag.Get("parquet"), ",", 2)
	if len(tag) < 1 || tag[0] == "" {
		return strings.ToLower(field.Name)
	}
	return tag[0]
}

// aggregatable is implemented by column types that allow usage of an aggregator.
type aggregatable interface {
	// canAggregate tells if this field SHOULD even be aggregated (if no aggregation is defined this is false).
	canAggregate() bool
	// aggregate consolidates the provided row values into one value according to the implemented aggregator function.
	aggregate([]parquet.Value) parquet.Value
}

// filterable is implemented by column types that allow custom value-per-value filters.
type filterable interface {
	// canFilter tells if this field SHOULD even be filtered (if no filter is defined this is false).
	canFilter() bool
	// filter checks an individual row value according to the implemented filter.
	filter(parquet.Value) bool
}

// boundable is implemented by column types that allow ranged index filters.
type boundable interface {
	// max returns a comparable representation of the maximum value boxed with any (nil means no max limit) (used for filter)
	max() any
	// min returns a comparable representation of the minimum value boxed with any (nil means no min limit) (used for filter)
	min() any

	// higher checks if the provided value is higher then the row value and if yes returns the value. (used for insertion)
	higher(any) (any, bool)
	// lower checks if the provided value is lower then the row value and if yes returns the value. (used for insertion)
	lower(any) (any, bool)
}
