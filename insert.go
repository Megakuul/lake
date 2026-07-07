package lake

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/megakuul/lakedb/internal/catalog"
	"github.com/parquet-go/parquet-go"
)

// Ingestor provides a processor for one batch of input data.
type Ingestor[T any] struct {
	table  string
	buffer *parquet.GenericBuffer[T]
	bucket *Bucket
	ranges map[string]catalog.Range
}

func NewIngestor[T any](bucket *Bucket) *Ingestor[T] {
	tableName, tableSorting := getMetadata(reflect.TypeFor[T]())
	return &Ingestor[T]{
		table: tableName,
		buffer: parquet.NewGenericBuffer[T](parquet.SortingRowGroupConfig(
			parquet.SortingColumns(tableSorting...),
		)),
		bucket: bucket,
		ranges: map[string]catalog.Range{},
	}
}

// insert writes the provided parquet row to the processor. This does NOT write anything to disk.
func (i *Ingestor[T]) Insert(ctx context.Context, row T) error {
	rowValue := reflect.ValueOf(row)
	if !rowValue.IsValid() {
		return fmt.Errorf("row type is invalid (expected non-nil struct)")
	}
	for columnMeta := range rowValue.Fields() {
		if !columnMeta.IsExported() {
			continue
		}
		columnName := getColumnName(columnMeta)

		if filter, ok := rowValue.FieldByIndex(columnMeta.Index).Interface().(boundable); ok {
			filterRange := i.ranges[columnName]
			if newMax, ok := filter.higher(filterRange.Max); ok {
				filterRange.Max = newMax
			}
			if newMin, ok := filter.lower(filterRange.Min); ok {
				filterRange.Min = newMin
			}
		}
	}

	_, err := i.buffer.Write([]T{row})
	return err
}

// Close writes the ingested rows into the underlying storage.
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
