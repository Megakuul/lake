package lakedb

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

func Query(ctx context.Context, bucket *Bucket, filter any) error {
	boundaries := newBoundaries()
	filterValue := reflect.ValueOf(filter)
	if !filterValue.IsValid() {
		return fmt.Errorf("invalid input filter type (expected table struct)")
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
			boundaries.Ints[fieldName] = IntBoundary{Max: field.filterMax, Min: field.filterMin}
		case Double:
			boundaries.Doubles[fieldName] = DoubleBoundary{Max: field.filterMax, Min: field.filterMin}
		}
	}

	err := bucket.Lookup(ctx, getTableName(filterValue), boundaries)
	if err != nil {
		return err
	}
	return nil
}
