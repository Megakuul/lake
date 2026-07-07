package lake

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// Table can be used as marker on any table to define metadata in the tag.
type Table struct{}

// getMetadata extracts metadata (name and sorting) from the provided table value.
func getMetadata(table reflect.Type) (name string, sorting []parquet.SortingColumn) {
	tableType := reflect.TypeFor[Table]()
	for field := range table.Fields() {
		if field.Type == tableType {
			name = field.Tag.Get("name")
			if field.Tag.Get("sort") == "" {
				break
			}
			for rawSorting := range strings.SplitSeq(field.Tag.Get("sort"), ",") {
				if column, ok := strings.CutSuffix(rawSorting, ":asc"); ok {
					sorting = append(sorting, parquet.Ascending(column, "data")) // data is added because lake primitives use .data as subpath
				} else if column, ok := strings.CutSuffix(rawSorting, ":desc"); ok {
					sorting = append(sorting, parquet.Descending(column, "data")) // data is added because lake primitives use .data as subpath
				} else {
					panic(fmt.Sprintf("invalid sorting spec on table: got '%s' expected '<column>:asc|desc'", rawSorting))
				}
			}
			break
		}
	}
	if name == "" {
		name = table.Name()
	}
	return name, sorting
}

// getColumnName extracts the parquet column name from a struct field.
// It is compatible to the parquet library struct tagging system.
func getColumnName(table reflect.StructField) string {
	tag := strings.SplitN(table.Tag.Get("parquet"), ",", 2)
	if len(tag) < 1 || tag[0] == "" {
		return table.Name
	}
	return tag[0]
}

// groupable is implemented by column types that allow custom grouping derivation functionality.
type groupable interface {
	canGroup() bool
	group(parquet.Value) parquet.Value
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
