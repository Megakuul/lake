package lake

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

func NewIngestor[T Table](bucket *Bucket) *Ingestor[T] {
	pseudo := *new(T)
	return &Ingestor[T]{
		table: pseudo.Name(),
		buffer: parquet.NewGenericBuffer[T](parquet.SortingRowGroupConfig(
			pseudo.Sorting(),
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

		if filter, ok := rowValue.FieldByIndex(fieldMeta.Index).Interface().(boundable); ok {
			filterRange := i.ranges[fieldName]
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
