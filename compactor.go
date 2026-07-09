package lake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/megakuul/lakedb/internal/catalog"
	"github.com/parquet-go/parquet-go"
)

type Compactor[T any] struct {
	table   string
	sorting []parquet.SortingColumn
	minSize int
	bucket  *Bucket
	ranges  map[string]catalog.Range
}

func NewCompactor[T any](bucket *Bucket, minSize int) *Compactor[T] {
	tableName, tableSorting := getMetadata(reflect.TypeFor[T]())
	return &Compactor[T]{
		table:   tableName,
		sorting: tableSorting,
		minSize: minSize,
		bucket:  bucket,
		ranges:  map[string]catalog.Range{},
	}
}

func (c *Compactor[T]) Compact(ctx context.Context) error {
	if time.Now().After(c.bucket.catalog.Expires) {
		if err := c.bucket.loadCatalog(ctx); err != nil {
			return err
		}
	}

	compactableShards := map[int]catalog.Shard{}

	err := c.bucket.commitCatalog(ctx, func(ref *catalog.Catalog) error {
		table := ref.Tables[c.table]
		schema := parquet.NewSchema(c.table, parquet.SchemaOf(*new(T)))

		batchSize := 0
		rowGroups := []parquet.RowGroup{}
		ranges := map[string]catalog.Range{}

		for i, shard := range table.Shards {
			if shard.Size > c.minSize {
				continue
			} else if batchSize > c.minSize {
				break // as soon as all shards to process generate a new shard that is > c.minSize we process.
			}
			compactableShards[i] = shard
			batchSize += shard.Size
			for column, columnRange := range shard.Ranges {
				currentRange, ok := ranges[column]
				if !ok {
					currentRange = columnRange
					continue
				}
				switch currentMin := currentRange.Min.(type) {
				case int64:
					if shardMin, ok := columnRange.Min.(int64); !ok || shardMin < currentMin {
						currentRange.Min = columnRange.Min
					}
				case float64:
					if shardMin, ok := columnRange.Min.(float64); !ok || shardMin < currentMin {
						currentRange.Min = columnRange.Min
					}
				case string:
					if shardMin, ok := columnRange.Min.(string); !ok || shardMin < currentMin {
						currentRange.Min = columnRange.Min
					}
				}
				switch currentMax := currentRange.Max.(type) {
				case int64:
					if shardMax, ok := columnRange.Max.(int64); !ok || shardMax > currentMax {
						currentRange.Max = columnRange.Max
					}
				case float64:
					if shardMax, ok := columnRange.Max.(float64); !ok || shardMax > currentMax {
						currentRange.Max = columnRange.Max
					}
				case string:
					if shardMax, ok := columnRange.Max.(string); !ok || shardMax > currentMax {
						currentRange.Max = columnRange.Max
					}
				}
				ranges[column] = currentRange
			}
		}

		for _, shard := range compactableShards {
			result, err := c.bucket.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &c.bucket.name,
				Key:    &shard.Target,
			})
			if err != nil {
				return err
			}
			defer result.Body.Close()
			buffer, err := io.ReadAll(result.Body)
			if err != nil {
				return err
			}
			file, err := parquet.OpenFile(bytes.NewReader(buffer), int64(shard.Size))
			if err != nil {
				return fmt.Errorf("cannot open shard file '%s': %v", shard.Target, err)
			}
			rowGroups = append(rowGroups, file.RowGroups()...)
		}
		rowGroup, err := parquet.MergeRowGroups(rowGroups, schema, parquet.SortingRowGroupConfig(parquet.SortingColumns(c.sorting...)))

		buffer := bytes.NewBuffer(nil)
		writer := parquet.NewGenericWriter[T](buffer)
		_, err = parquet.CopyRows(writer, rowGroup.Rows())
		if err != nil {
			return fmt.Errorf("failed to flush parquet buffer: %v", err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("failed to flush parquet writer: %v", err)
		}

		target := path.Join(c.table, uuid.New().String()+".parquet")
		_, err = c.bucket.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &c.bucket.name,
			Key:         &target,
			IfNoneMatch: new("*"),
			Body:        buffer,
		})
		if err != nil {
			return err
		}
		shard := catalog.Shard{
			Size:   buffer.Len(),
			Target: target,
			// Ranges: ranges,
		}
		newShards := make([]catalog.Shard, 0, len(table.Shards))
		for i, shard := range table.Shards {
			if _, ok := compactableShards[i]; ok {
				continue
			}
			newShards = append(newShards, shard)
		}
		newShards = append(newShards, shard)
		table.Shards = newShards
		ref.Tables[c.table] = table
		return nil
	})
	if err != nil {
		return fmt.Errorf("compaction: %w", err)
	}

	var shredErr error
	for _, shard := range compactableShards {
		_, err = c.bucket.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &c.bucket.name,
			Key:    &shard.Target,
		})
		if err != nil {
			shredErr = errors.Join(err, err)
		}
	}
	if shredErr != nil {
		return fmt.Errorf("compaction cleanup: %w", shredErr)
	}
	return nil
}
