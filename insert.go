package lake

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/megakuul/lakedb/internal/catalog"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// Ingestor provides a processor for one batch of input data.
type Ingestor[T any] struct {
	table      string
	bucket     *Bucket
	buffer     *bytes.Buffer
	writer     *parquet.SortingWriter[T]
	newWriter  func(*bytes.Buffer) *parquet.SortingWriter[T]
	lastCommit time.Time
	writerLock sync.RWMutex

	maxDuration time.Duration
}

type IngestorOption[T any] func(i *Ingestor[T])

func NewIngestor[T any](bucket *Bucket, opts ...IngestorOption[T]) *Ingestor[T] {
	tableName, tableSorting := getMetadata(reflect.TypeFor[T]())
	i := &Ingestor[T]{
		table:       tableName,
		buffer:      bytes.NewBuffer(nil),
		bucket:      bucket,
		lastCommit:  time.Now(),
		maxDuration: -1,
	}
	i.newWriter = func(buffer *bytes.Buffer) *parquet.SortingWriter[T] {
		return parquet.NewSortingWriter[T](buffer, 100_000, parquet.SortingWriterConfig(
			parquet.SortingColumns(tableSorting...),
		))
	}
	i.writer = i.newWriter(i.buffer)
	for _, opt := range opts {
		opt(i)
	}
	return i
}

// WithAutoCommit ensures that inserts automatically commit data after the specified interval.
func WithAutoCommit[T any](interval time.Duration) IngestorOption[T] {
	return func(i *Ingestor[T]) {
		i.maxDuration = interval
	}
}

// Insert writes the provided parquet row to the processor. This does NOT write anything to disk.
func (i *Ingestor[T]) Insert(ctx context.Context, rows ...T) error {
	i.writerLock.RLock()
	if _, err := i.writer.Write(rows); err != nil {
		i.writerLock.RUnlock()
		return err
	}
	if i.maxDuration != -1 && time.Now().Add(i.maxDuration).After(i.lastCommit) {
		i.writerLock.RUnlock()
		return i.Commit(ctx)
	} else {
		i.writerLock.RUnlock()
		return nil
	}
}

// Commit writes the processed parquet rows to disk.
// This temporarily locks ingestion, writes to disk and replaces the underlying writer with a new one (parquet files are immutable).
// After commit is done inserts proceed on the new writer.
func (i *Ingestor[T]) Commit(ctx context.Context) error {
	i.writerLock.Lock()
	defer i.writerLock.Unlock()

	if err := i.writer.Close(); err != nil {
		return fmt.Errorf("failed to flush parquet writer: %v", err)
	}
	schema := parquet.NewSchema(i.table, parquet.SchemaOf(*new(T)))
	ranges, err := extractRanges(schema, i.writer.File().Metadata().RowGroups)
	if err != nil {
		return fmt.Errorf("extracting ranges: %v", err)
	}
	if err = i.bucket.write(ctx, i.table, i.buffer.Bytes(), ranges); err != nil {
		return fmt.Errorf("bucket writer: %v", err)
	}
	i.buffer.Reset()
	i.writer = i.newWriter(i.buffer)
	i.lastCommit = time.Now()
	return nil
}

// Close writes the ingested rows into the underlying storage.
func (i *Ingestor[T]) Close(ctx context.Context) error {
	return i.Commit(ctx)
}

// extractRanges reads the metadata from the provided row groups to calculate the catalog ranges per row.
func extractRanges(schema *parquet.Schema, rowGroups thrift.Slice[format.RowGroup]) (map[string]catalog.Range, error) {
	ranges := map[string]catalog.Range{}
	for _, rowGroup := range rowGroups {
		for column, chunk := range rowGroup.Columns {
			columnName := schema.Columns()[column][0]
			columnRange := ranges[columnName]

			leaf, _ := schema.Lookup(chunk.MetaData.PathInSchema...)
			kind := leaf.Node.Type().Kind()

			stats := chunk.MetaData.Statistics

			max := kind.Value(stats.MaxValue)
			switch max.Kind() {
			case parquet.Int64:
				if currentMax, ok := columnRange.Max.(int64); !ok || currentMax < max.Int64() {
					columnRange.Max = max.Int64()
				}
			case parquet.Double:
				if currentMax, ok := columnRange.Max.(float64); !ok || currentMax < max.Double() {
					columnRange.Max = max.Double()
				}
			case parquet.ByteArray:
				if currentMax, ok := columnRange.Max.(string); !ok || currentMax < string(max.ByteArray()) {
					columnRange.Max = string(max.ByteArray())
				}
			}
			min := kind.Value(stats.MinValue)
			switch min.Kind() {
			case parquet.Int64:
				if currentMin, ok := columnRange.Min.(int64); !ok || currentMin > min.Int64() {
					columnRange.Min = min.Int64()
				}
			case parquet.Double:
				if currentMin, ok := columnRange.Min.(float64); !ok || currentMin > min.Double() {
					columnRange.Min = min.Double()
				}
			case parquet.ByteArray:
				if currentMin, ok := columnRange.Min.(string); !ok || currentMin > string(min.ByteArray()) {
					columnRange.Min = string(min.ByteArray())
				}
			}
			ranges[columnName] = columnRange
		}
	}
	return ranges, nil
}
