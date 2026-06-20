package lakedb

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"
)

func Query[T any](ctx context.Context, bucket *Bucket, filter T) ([]T, error) {
	boundaries := newBoundaries()
	filters := map[string]checkFilter{}
	filterValue := reflect.ValueOf(filter)
	if !filterValue.IsValid() {
		return nil, fmt.Errorf("invalid input filter type (expected table struct)")
	}
	for fieldMeta := range filterValue.Fields() {
		if !fieldMeta.IsExported() {
			continue
		}
		fieldName := ""
		tag := strings.SplitN(fieldMeta.Tag.Get("parquet"), ",", 1)
		if len(tag) < 1 || tag[0] == "" {
			fieldName = fieldMeta.Name
		} else {
			fieldName = tag[0]
		}
		switch field := filterValue.FieldByIndex(fieldMeta.Index).Interface().(type) {
		case Int:
			boundaries.Ints[fieldName] = IntBoundary{Max: field.filter.max, Min: field.filter.min}
			filters[fieldName] = func(v parquet.Value) bool {
				if v.Kind() != parquet.Int64 {
					return true
				}
				for _, op := range field.filter.filterOps {
					if !op(v.Int64()) {
						return false
					}
				}
				return true
			}
		case Double:
			boundaries.Doubles[fieldName] = DoubleBoundary{Max: field.filter.max, Min: field.filter.min}
			filters[fieldName] = func(v parquet.Value) bool {
				if v.Kind() != parquet.Double {
					return true
				}
				for _, op := range field.filter.filterOps {
					if !op(v.Double()) {
						return false
					}
				}
				return true
			}
		case String:
			filters[fieldName] = func(v parquet.Value) bool {
				if v.Kind() != parquet.ByteArray {
					return true
				}
				for _, op := range field.filter.filterOps {
					if !op(string(v.ByteArray())) {
						return false
					}
				}
				return true
			}
		}
	}

	rows, err := lookup[T](ctx, bucket, getTableName(filterValue), boundaries, filters)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
