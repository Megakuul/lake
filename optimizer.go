package lake

import (
	"fmt"
	"slices"
	"strings"

	"github.com/megakuul/lakedb/internal/catalog"
	"github.com/parquet-go/parquet-go"
)

// filterColumn describes a column in context of a filter operation.
type filterColumn struct {
	weight     int // lower means execute earlier
	index      int
	name       string
	chunkCheck func(parquet.Value) bool
	chunkRange catalog.Range
}

// createFilterColumns uses the provided schema and the context query to create an optimized sequence of filterColumns.
// The filterColumns should be processed in the provided order (this is the calculated optimal way).
func createFilterColumns(schema *parquet.Schema, q *query) ([]filterColumn, error) {
	filterColumns := []filterColumn{}
	for i, column := range schema.Columns() {
		columnName := column[0]
		leaf, ok := schema.Lookup(column...)
		if !ok {
			return nil, fmt.Errorf("table contains corrupted column schema '%s' ", strings.Join(column, "."))
		}
		weight := 0

		switch leaf.Node.Type().Kind() {
		case parquet.Double, parquet.Int64:
			weight += 2
		case parquet.ByteArray:
			weight += 10
		}

		chunkCheck, checkAvailable := q.checks[columnName]
		chunkRange := q.ranges[columnName]
		if !checkAvailable && chunkRange.Max == nil && chunkRange.Min == nil {
			continue // no filter op or range defined; skip column entirely
		}

		if chunkRange.Min == nil {
			weight += 10
		}
		if chunkRange.Max == nil {
			weight += 10
		}

		filterColumns = append(filterColumns, filterColumn{
			weight:     weight,
			index:      i,
			name:       columnName,
			chunkCheck: chunkCheck,
			chunkRange: chunkRange,
		})
	}
	return slices.SortedFunc(slices.Values(filterColumns), func(a, b filterColumn) int {
		return a.weight - b.weight
	}), nil
}
