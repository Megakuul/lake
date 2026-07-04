package lake

import (
	"context"
	"fmt"
	"maps"
	"reflect"

	"github.com/megakuul/lakedb/catalog"
	"github.com/parquet-go/parquet-go"
)

// QueryBuilder wraps the query structure with api exposed methods to construct it.
// Generics are not strictly required here but make the api more userfriendly for autocompletion.
type QueryBuilder[T Table] struct {
	query
}

func Query[T Table]() *QueryBuilder[T] {
	return &QueryBuilder[T]{query: query{
		ranges:      map[string]catalog.Range{},
		checks:      map[string]func(parquet.Value) bool{},
		limit:       -1,
		grouping:    map[string]func(parquet.Value) (string, parquet.Value){},
		aggregators: map[string]func([]parquet.Value) parquet.Value{},
	}}
}

func (b *QueryBuilder[T]) Limit(limit int) *QueryBuilder[T] {
	b.limit = limit
	return b
}

func (b *QueryBuilder[T]) Where(filter T) *QueryBuilder[T] {
	ranges := map[string]catalog.Range{}
	checks := map[string]func(parquet.Value) bool{}
	filterValue := reflect.ValueOf(filter)
	if !filterValue.IsValid() {
		panic("invalid input filter type (expected table struct)")
	}
	for columnMeta := range filterValue.Fields() {
		if !columnMeta.IsExported() {
			continue
		}
		columnName := getColumnName(columnMeta)
		if filter, ok := filterValue.FieldByIndex(columnMeta.Index).Interface().(boundable); ok {
			ranges[columnName] = catalog.Range{Max: filter.max(), Min: filter.min()}
		}
		if filter, ok := filterValue.FieldByIndex(columnMeta.Index).Interface().(filterable); ok && filter.canFilter() {
			checks[columnName] = filter.filter
		}
	}
	maps.Copy(b.ranges, ranges)
	maps.Copy(b.checks, checks)
	return b
}

func (b *QueryBuilder[T]) GroupBy(grouping T) *QueryBuilder[T] {
	groupingValue := reflect.ValueOf(grouping)
	if !groupingValue.IsValid() {
		panic("invalid input grouping type (expected table struct)")
	}
	for columnMeta := range groupingValue.Fields() {
		if !columnMeta.IsExported() {
			continue
		}
		columnName := getColumnName(columnMeta)
		if grouper, ok := groupingValue.FieldByIndex(columnMeta.Index).Interface().(groupable); ok && grouper.canGroup() {
			b.grouping[columnName] = grouper.group
		}
	}
	return b
}

func (b *QueryBuilder[T]) Aggregate(aggregates T) *QueryBuilder[T] {
	aggregatesValue := reflect.ValueOf(aggregates)
	if !aggregatesValue.IsValid() {
		panic("invalid input aggregates type (expected table struct)")
	}
	for columnMeta := range aggregatesValue.Fields() {
		if !columnMeta.IsExported() {
			continue
		}
		columnName := getColumnName(columnMeta)
		if aggregator, ok := aggregatesValue.FieldByIndex(columnMeta.Index).Interface().(aggregatable); ok && aggregator.canAggregate() {
			b.aggregators[columnName] = aggregator.aggregate
		}
	}
	return b
}

func (b *QueryBuilder[T]) Scan(ctx context.Context, bucket *Bucket) ([]T, error) {
	pseudo := *new(T)
	schema := parquet.NewSchema(pseudo.Name(), parquet.SchemaOf(pseudo))
	rows, err := bucket.aggregate(ctx, schema, &b.query)
	if err != nil {
		return nil, err
	}
	result := make([]T, 0, len(rows))
	for _, row := range rows {
		var outputRow T
		if err = schema.Reconstruct(&outputRow, row); err != nil {
			return nil, fmt.Errorf("failed to deserialize row: %v", err)
		}
		result = append(result, outputRow)
	}
	return result, nil
}
