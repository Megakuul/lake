package lake

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/megakuul/lake/internal/catalog"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// Ingestor provides a processor for one batch of input data.
type Ingestor[T any] struct {
	table          string
	bucket         *Bucket
	buffer         *bytes.Buffer
	backlog        []ingestShard
	maxBacklogSize int
	writer         *parquet.SortingWriter[T]
	newWriter      func(*bytes.Buffer) *parquet.SortingWriter[T]
	lastCommit     time.Time
	writerLock     sync.Mutex

	maxDuration time.Duration
}

// ingestShard describes a compiled shard unit that waits to be written to storage.
type ingestShard struct {
	ranges map[string]catalog.Range
	data   []byte
}

type IngestorOption[T any] func(i *Ingestor[T])

func NewIngestor[T any](bucket *Bucket, opts ...IngestorOption[T]) *Ingestor[T] {
	tableName, tableSorting := getMetadata(reflect.TypeFor[T]())
	i := &Ingestor[T]{
		table:          tableName,
		buffer:         bytes.NewBuffer(nil),
		backlog:        []ingestShard{},
		maxBacklogSize: 100,
		bucket:         bucket,
		lastCommit:     time.Now(),
		maxDuration:    -1,
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

// WithMaxBacklogSize defines a custom maximum queue size of failing writes that are kept in backlog.
func WithMaxBacklogSize[T any](size int) IngestorOption[T] {
	return func(i *Ingestor[T]) {
		i.maxBacklogSize = size
	}
}

// WithAutoCommit ensures that inserts automatically commit data after the specified interval.
func WithAutoCommit[T any](interval time.Duration) IngestorOption[T] {
	return func(i *Ingestor[T]) {
		i.maxDuration = interval
	}
}

// Insert writes the provided parquet row to the processor. This does NOT write anything to disk.
func (in *Ingestor[T]) Insert(ctx context.Context, rows ...T) error {
	in.writerLock.Lock()
	defer in.writerLock.Unlock()

	if _, err := in.writer.Write(rows); err != nil {
		return err
	}
	if in.maxDuration != -1 && time.Now().Add(-in.maxDuration).After(in.lastCommit) {
		return in.commit(ctx)
	} else {
		return nil
	}
}

// Commit writes the processed parquet rows to disk.
func (in *Ingestor[T]) Commit(ctx context.Context) error {
	in.writerLock.Lock()
	defer in.writerLock.Unlock()
	return in.commit(ctx)
}

// commit writes the processed parquet rows to disk (requires a writer lock to be already present).
// This temporarily locks ingestion, writes to disk and replaces the underlying writer with a new one (parquet files are immutable).
// After commit is done inserts proceed on the new writer.
func (in *Ingestor[T]) commit(ctx context.Context) error {
	if len(in.backlog) > in.maxBacklogSize {
		in.backlog = in.backlog[1:]
	}
	defer func() {
		in.buffer.Reset()
		in.writer = in.newWriter(in.buffer)
		in.lastCommit = time.Now()
	}()

	if err := in.writer.Close(); err != nil {
		return fmt.Errorf("failed to flush parquet writer: %v", err)
	}
	schema := parquet.NewSchema(in.table, parquet.SchemaOf(*new(T)))
	ranges, err := extractRanges(schema, in.writer.File().Metadata().RowGroups)
	if err != nil {
		return fmt.Errorf("extracting ranges: %v", err)
	}
	if in.writer.File().NumRows() > 0 {
		in.backlog = append(in.backlog, ingestShard{
			ranges: ranges,
			data:   bytes.Clone(in.buffer.Bytes()),
		})
	}

	for i, shard := range in.backlog {
		if err = in.bucket.write(ctx, in.table, shard.data, shard.ranges); err != nil {
			in.backlog = in.backlog[i:]
			return fmt.Errorf("bucket writer: %v", err)
		}
	}
	in.backlog = nil
	return nil
}

// Close writes the ingested rows into the underlying storage.
func (in *Ingestor[T]) Close(ctx context.Context) error {
	return in.Commit(ctx)
}

// extractRanges reads the metadata from the provided row groups to calculate the catalog ranges per row.
func extractRanges(schema *parquet.Schema, rowGroups thrift.Slice[format.RowGroup]) (map[string]catalog.Range, error) {
	ranges := map[string]catalog.Range{}
	for _, rowGroup := range rowGroups {
		for column, chunk := range rowGroup.Columns {
			columnName := schema.Columns()[column][0]
			columnRange, update := ranges[columnName]

			leaf, _ := schema.Lookup(chunk.MetaData.PathInSchema...)
			kind := leaf.Node.Type().Kind()

			stats := chunk.MetaData.Statistics

			max, min := kind.Value(stats.MaxValue), kind.Value(stats.MinValue)
			switch kind {
			case parquet.Int64:
				columnRange.Kind = catalog.ColumnInt
				if !update || columnRange.MaxInt < max.Int64() {
					columnRange.MaxEnabled = true
					columnRange.MaxInt = max.Int64()
				}
				if !update || columnRange.MinInt > min.Int64() {
					columnRange.MinEnabled = true
					columnRange.MinInt = min.Int64()
				}
			case parquet.Double:
				columnRange.Kind = catalog.ColumnFloat
				// json cannot encode +-Inf / NaN; clamp so the catalog stays serializable.
				maxFloat := math.Max(-math.MaxFloat64, math.Min(math.MaxFloat64, max.Double()))
				minFloat := math.Max(-math.MaxFloat64, math.Min(math.MaxFloat64, min.Double()))
				if !update || columnRange.MaxFloat < maxFloat {
					columnRange.MaxEnabled = true
					columnRange.MaxFloat = maxFloat
				}
				if !update || columnRange.MinFloat > minFloat {
					columnRange.MinEnabled = true
					columnRange.MinFloat = minFloat
				}
			case parquet.ByteArray:
				columnRange.Kind = catalog.ColumnString
				if !update || columnRange.MaxString < string(max.ByteArray()) {
					columnRange.MaxEnabled = true
					columnRange.MaxString = string(max.ByteArray())
				}
				if !update || columnRange.MinString > string(min.ByteArray()) {
					columnRange.MinEnabled = true
					columnRange.MinString = string(min.ByteArray())
				}
			}
			ranges[columnName] = columnRange
		}
	}
	return ranges, nil
}
