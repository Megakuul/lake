package lakedb

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"
)

type Ingestor[T any] struct {
	table      string
	buffer     *bytes.Buffer
	writer     *parquet.GenericWriter[T]
	bucket     *Bucket
	boundaries Boundaries
}

func NewIngestor[T any](bucket *Bucket) *Ingestor[T] {
	buffer := bytes.NewBuffer(nil)
	return &Ingestor[T]{
		table:      getTableName(reflect.ValueOf(*new(T))),
		buffer:     buffer,
		writer:     parquet.NewGenericWriter[T](buffer),
		bucket:     bucket,
		boundaries: newBoundaries(),
	}
}

func (i *Ingestor[T]) Insert(ctx context.Context, row T) error {
	rowValue := reflect.ValueOf(row)
	if !rowValue.IsValid() {
		return fmt.Errorf("row type is invalid (expected non-nil struct)")
	}
	for fieldMeta := range rowValue.Fields() {
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
		switch field := rowValue.FieldByIndex(fieldMeta.Index).Interface().(type) {
		case Int:
			boundary, ok := i.boundaries.Ints[fieldName]
			if !ok {
				boundary = IntBoundary{Max: new(int64), Min: new(int64)}
				i.boundaries.Ints[fieldName] = boundary
			}
			if *boundary.Max < field.data { // here is nil deref
				*boundary.Max = field.data
			}
			if *boundary.Min > field.data {
				*boundary.Min = field.data
			}
			i.boundaries.Ints[fieldName] = boundary
		case Double:
			boundary, ok := i.boundaries.Doubles[fieldName]
			if !ok {
				boundary = DoubleBoundary{Max: new(float64), Min: new(float64)}
				i.boundaries.Doubles[fieldName] = boundary
			}
			if *boundary.Max < field.data {
				*boundary.Max = field.data
			}
			if *boundary.Min > field.data {
				*boundary.Min = field.data
			}
			i.boundaries.Doubles[fieldName] = boundary
		default:
		}
	}
	_, err := i.writer.Write([]T{row})
	return err
}

func (i *Ingestor[T]) Close(ctx context.Context) error {
	if err := i.writer.Close(); err != nil {
		return fmt.Errorf("failed to flush parquet writer: %v", err)
	}
	return i.bucket.Write(ctx, i.table, i.buffer.Bytes(), i.boundaries)
}
