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
}

func NewCompactor[T any](bucket *Bucket, minSize int) *Compactor[T] {
	tableName, tableSorting := getMetadata(reflect.TypeFor[T]())
	return &Compactor[T]{
		table:   tableName,
		sorting: tableSorting,
		minSize: minSize,
		bucket:  bucket,
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

		rowGroups := []parquet.RowGroup{}
		for i, shard := range table.Shards {
			if shard.Size > c.minSize {
				continue
			}
			compactableShards[i] = shard
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
		if err != nil {
			return fmt.Errorf("merge shards: %v", err)
		}

		reader := rowGroup.Rows()
		defer reader.Close()

		newShards := make([]catalog.Shard, 0, len(table.Shards))
		for i, shard := range table.Shards {
			if _, ok := compactableShards[i]; ok {
				continue
			}
			newShards = append(newShards, shard)
		}
		proceed := true
		for proceed {
			buffer := bytes.NewBuffer(nil)
			writer := parquet.NewGenericWriter[T](buffer, parquet.SortingWriterConfig(parquet.SortingColumns(c.sorting...)))
			for proceed && writer.Size() < int64(c.minSize) {
				rowBuffer := make([]parquet.Row, 1000)
				n, err := reader.ReadRows(rowBuffer)
				if err != nil {
					if errors.Is(err, io.EOF) {
						proceed = false
					} else {
						return fmt.Errorf("failed to read parquet row batch: %v", err)
					}
				}
				_, err = writer.WriteRows(rowBuffer[:n])
				if err != nil {
					return fmt.Errorf("failed to write parquet row batch: %v", err)
				}
			}
			if err := writer.Close(); err != nil {
				return fmt.Errorf("failed to flush parquet writer: %v", err)
			}
			ranges, err := extractRanges(schema, writer.File().Metadata().RowGroups)
			if err != nil {
				return fmt.Errorf("extracting ranges: %v", err)
			}

			shardSize := buffer.Len()
			target := path.Join(c.table, uuid.New().String()+".parquet")
			_, err = c.bucket.client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      &c.bucket.name,
				Key:         &target,
				IfNoneMatch: new("*"),
				Body:        bytes.NewReader(buffer.Bytes()),
			})
			if err != nil {
				return err
			}
			shard := catalog.Shard{
				Size:   shardSize,
				Target: target,
				Ranges: ranges,
			}
			newShards = append(newShards, shard)
		}

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
