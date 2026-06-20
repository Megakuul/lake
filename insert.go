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
		tag := strings.SplitN(fieldMeta.Tag.Get("parquet"), ",", 2)
		if len(tag) < 2 || tag[0] == "" {
			fieldName = fieldMeta.Name
		} else {
			fieldName = tag[0]
		}
		switch field := rowValue.FieldByIndex(fieldMeta.Index).Interface().(type) {
		case Int:
			boundary := i.boundaries.Ints[fieldName]
			if boundary.Max == nil || *boundary.Max < field.Data {
				boundary.Max = &field.Data
			}
			if boundary.Min == nil || *boundary.Min > field.Data {
				boundary.Min = &field.Data
			}
			i.boundaries.Ints[fieldName] = boundary
		case Double:
			boundary := i.boundaries.Doubles[fieldName]
			if boundary.Max == nil || *boundary.Max < field.Data {
				boundary.Max = &field.Data
			}
			if boundary.Min == nil || *boundary.Min > field.Data {
				boundary.Min = &field.Data
			}
			i.boundaries.Doubles[fieldName] = boundary
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
