package lakedb

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/megakuul/lakedb/catalog"
	"github.com/parquet-go/parquet-go"
)

type Ingestor[T Table] struct {
	table  string
	buffer *parquet.GenericBuffer[T]
	bucket *Bucket
	ranges map[string]catalog.Range
}

func NewIngestor[T Table](bucket *Bucket, sample T) *Ingestor[T] {
	return &Ingestor[T]{
		table: sample.Name(),
		buffer: parquet.NewGenericBuffer[T](parquet.SortingRowGroupConfig(
			parquet.SortingColumns(
				parquet.Ascending("timestamp"),
			),
		)),
		bucket: bucket,
		ranges: map[string]catalog.Range{},
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
			fieldName = strings.ToLower(fieldMeta.Name)
		} else {
			fieldName = tag[0]
		}
		switch field := rowValue.FieldByIndex(fieldMeta.Index).Interface().(type) {
		case Int:
			filterRange := i.ranges[fieldName]
			// TODO check if filterRange is actually int64
			if filterRange.Max == nil || filterRange.Max.(int64) < field.Data {
				filterRange.Max = field.Data
			}
			if filterRange.Min == nil || filterRange.Min.(int64) > field.Data {
				filterRange.Min = field.Data
			}
			i.ranges[fieldName] = filterRange
		case Double:
			filterRange := i.ranges[fieldName]
			if filterRange.Max == nil || filterRange.Max.(float64) < field.Data {
				filterRange.Max = field.Data
			}
			if filterRange.Min == nil || filterRange.Min.(float64) > field.Data {
				filterRange.Min = field.Data
			}
			i.ranges[fieldName] = filterRange
		}
	}

	_, err := i.buffer.Write([]T{row})
	return err
}

func (i *Ingestor[T]) Close(ctx context.Context) error {
	sort.Sort(i.buffer)

	output := bytes.NewBuffer(nil)
	writer := parquet.NewGenericWriter[T](output)
	_, err := parquet.CopyRows(writer, i.buffer.Rows())
	if err != nil {
		return fmt.Errorf("failed to flush parquet buffer: %v", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to flush parquet writer: %v", err)
	}
	return i.bucket.write(ctx, i.table, output.Bytes(), i.ranges)
}
