package lakedb

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
		grouping:    []map[string]func(parquet.Value) bool{},
		aggregators: []map[string]func([]parquet.Value) parquet.Value{},
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
		if filter, ok := filterValue.FieldByIndex(columnMeta.Index).Interface().(rangeFilter); ok {
			ranges[columnName] = catalog.Range{Max: filter.max(), Min: filter.min()}
		}
		if filter, ok := filterValue.FieldByIndex(columnMeta.Index).Interface().(genericFilter); ok {
			checks[columnName] = filter.filter
		}
	}
	maps.Copy(b.ranges, ranges)
	maps.Copy(b.checks, checks)
	return b
}

func (b *QueryBuilder[T]) Scan(ctx context.Context, bucket *Bucket) ([]T, error) {
	// use one empty group (match everything into the group) for the scan.
	b.grouping = []map[string]func(parquet.Value) bool{{}}

	pseudo := *new(T)
	schema := parquet.NewSchema(pseudo.Name(), parquet.SchemaOf(pseudo))
	groups, err := bucket.lookup(ctx, schema, &b.query)
	if err != nil {
		return nil, err
	}
	result := make([]T, 0, len(groups[0]))
	for _, row := range groups[0] {
		var outputRow T
		if err = schema.Reconstruct(&outputRow, row); err != nil {
			return nil, fmt.Errorf("failed to deserialize row: %v", err)
		}
		result = append(result, outputRow)
	}
	return result, nil
}

func (b *QueryBuilder[T]) Aggregate(ctx context.Context, bucket *Bucket, windows []T) error {
	b.grouping = []map[string]func(parquet.Value) bool{}
	b.aggregators = []map[string]func([]parquet.Value) parquet.Value{}

	for i, window := range windows {
		b.grouping = append(b.grouping, map[string]func(parquet.Value) bool{})
		b.aggregators = append(b.aggregators, map[string]func([]parquet.Value) parquet.Value{})

		windowValue := reflect.ValueOf(window)
		if !windowValue.IsValid() {
			panic("invalid aggregator window type (expected struct)")
		}
		for columnMeta := range windowValue.Fields() {
			if !columnMeta.IsExported() {
				continue
			}
			columnName := getColumnName(columnMeta)
			if filter, ok := windowValue.FieldByIndex(columnMeta.Index).Interface().(genericFilter); ok {
				b.grouping[i][columnName] = filter.filter
			}
			if aggregator, ok := windowValue.FieldByIndex(columnMeta.Index).Interface().(genericAggregator); ok {
				b.aggregators[i][columnName] = aggregator.aggregate
			}
		}
	}

	pseudo := *new(T)
	schema := parquet.NewSchema(pseudo.Name(), parquet.SchemaOf(pseudo))
	groups, err := bucket.lookup(ctx, schema, &b.query)
	if err != nil {
		return err
	}
	for i, group := range groups {
		if err = schema.Reconstruct(&windows[i], group[0]); err != nil {
			return fmt.Errorf("failed to deserialize row: %v", err)
		}
	}
	return nil
}
