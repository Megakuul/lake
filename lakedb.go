package lakedb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

type Bucket struct {
	client      *s3.Client
	name        string
	catalog     Catalog
	catalogLock sync.RWMutex
}

type BucketOption func(*s3.Options)

// New constructs a lakedb bucket pointing to the provided s3 url / bucket.
// Credentials are loaded with aws sdk (e.g. from env AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY).
func New(ctx context.Context, url, bucket string, opts ...BucketOption) (*Bucket, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithHTTPClient(&http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(url)
		o.UsePathStyle = true
		for _, opt := range opts {
			opt(o)
		}
	})

	return NewFromClient(ctx, client, bucket)
}

// WithCredentials specifies a static access and secret key.
// This disables the default AWS SDK credential process.
func WithCredentials(accessKey, secretKey string) BucketOption {
	return func(o *s3.Options) {
		o.Credentials = credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	}
}

// WithRegion specifies a static bucket region.
// This disables the default AWS SDK region process.
func WithRegion(region string) BucketOption {
	return func(o *s3.Options) {
		o.Region = region
	}
}

// NewFromClient initializes a dynamitedb bucket from an existing aws s3 sdk client.
func NewFromClient(ctx context.Context, client *s3.Client, bucket string) (*Bucket, error) {
	b := &Bucket{
		name: bucket,
		catalog: Catalog{
			Key:    "lakedb.catalog",
			ETag:   nil,
			Tables: map[string]Table{},
		},
		catalogLock: sync.RWMutex{},
		client:      client,
	}
	return b, b.loadCatalog(ctx)
}

// checkNumeralBoundary checks if the originalMin - originalMax range is INSIDE the filter range.
// Filters are optional, if one side is omitted everything on this side matches
// e.g. max == nil means original range must be between min - ∞.
func checkNumeralBoundary[T int64 | float64](originalMin, originalMax T, filterMin, filterMax *T) bool {
	if filterMin != nil && *filterMin > originalMax {
		return false
	}
	if filterMax != nil && *filterMax < originalMin {
		return false
	}
	return true
}

func (b *Bucket) Lookup(ctx context.Context, tableName string, filter Boundaries) error {
	b.catalogLock.RLock()
	defer b.catalogLock.RUnlock()
	shards := []Shard{}
	table, ok := b.catalog.Tables[tableName]
	if !ok {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}
	for _, shard := range table.Shards {
		for name, field := range shard.Boundaries.Ints {
			fieldFilter, ok := filter.Ints[name]
			if ok && field.Min != nil && field.Max != nil {
				if !checkNumeralBoundary(*field.Min, *field.Max, fieldFilter.Min, fieldFilter.Max) {
					break
				}
			}
		}
		for name, field := range shard.Boundaries.Doubles {
			fieldFilter, ok := filter.Doubles[name]
			if ok && field.Min != nil && field.Max != nil {
				if !checkNumeralBoundary(*field.Min, *field.Max, fieldFilter.Min, fieldFilter.Max) {
					break
				}
			}
		}
		shards = append(shards, shard)
	}

	for _, shard := range shards {
		reader := newReader(ctx, b.client, b.name, shard.Target)
		file, err := parquet.OpenFile(reader, int64(shard.Size))
		if err != nil {
			return fmt.Errorf("cannot open shard file '%s': %v", shard.Target, err)
		}

		println("column idx")
		println(len(file.ColumnIndexes()))
		println("offset idx")
		println(len(file.OffsetIndexes()))
		for _, offset := range file.OffsetIndexes() {
			for _, location := range offset.PageLocations {
				_ = location
			}
		}
	}
	return fmt.Errorf("alarm")
	// 	for _, column := range file.ColumnIndexes() {
	// 		_ = column
	// 		// file.Root().Pages().SeekToRow()
	// 	}
	// 	for _, rg := range file.Metadata().RowGroups {
	// 		for i, column := range rg.Columns {
	// 			switch column.MetaData.Type {
	// 			case format.Int64:
	// 				fieldBoundary, ok := intBoundary[file.Root().Columns()[i].Name()]
	// 				max := binary.LittleEndian.Uint64(column.MetaData.Statistics.MinValue)
	// 				if ok && max <= fieldBoundary.Min || min >= fieldBoundary.Max {
	// 				}
	// 			case format.Double:
	//
	// 			}
	// 			file.Root().Columns()[i].Name()
	//
	// 			column.MetaData.Type == parquet.Int64Type
	// 			if ok {
	// 				if column.MetaData.Statistics.Min <= filterField.Max && field.Max >= filterField.Min {
	// 				}
	// 			}
	// 			column.MetaData.Statistics.Max
	// 		}
	// 	}
	// }
	return nil
}

func (b *Bucket) Write(ctx context.Context, tableName string, data []byte, boundaries Boundaries) error {
	target := path.Join(tableName, uuid.New().String()+".parquet")
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &b.name,
		Key:         &target,
		IfNoneMatch: new("*"),
		Body:        bytes.NewReader(data),
	})
	if err != nil {
		return err
	}

	shard := Shard{
		Size:       len(data),
		Target:     target,
		Boundaries: boundaries,
	}
	modification := func(c *Catalog) error {
		table := c.Tables[tableName]
		table.Shards = append(table.Shards, shard)
		c.Tables[tableName] = table
		return nil
	}
	if err = b.commitCatalog(ctx, modification); err != nil {
		// retry once on optimistic lock failure
		if errors.Is(err, ErrOptimisticLock) {
			if err := b.loadCatalog(ctx); err != nil {
				return err
			}
			return b.commitCatalog(ctx, modification)
		}
		return err
	}
	return nil
}

// loadCatalog loads the current catalog from datastore into the engine.
// It creates the catalog if not existent.
func (b *Bucket) loadCatalog(ctx context.Context) error {
	b.catalogLock.Lock()
	defer b.catalogLock.Unlock()
	result, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &b.name,
		Key:    &b.catalog.Key,
	})
	if err != nil {
		if _, ok := errors.AsType[*types.NoSuchKey](err); ok {
			rawCatalog, err := json.Marshal(b.catalog)
			if err != nil {
				return err
			}
			result, err := b.client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      &b.name,
				Key:         &b.catalog.Key,
				IfNoneMatch: new("*"),
				Body:        bytes.NewReader(rawCatalog),
			})
			if err != nil {
				return err
			}
			b.catalog.ETag = result.ETag
			return nil
		}
		return err
	}
	defer result.Body.Close()
	rawCatalog, err := io.ReadAll(result.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(rawCatalog, &b.catalog)
	if err != nil {
		return err
	}
	b.catalog.ETag = result.ETag
	return nil
}

var ErrOptimisticLock = errors.New("optimistic lock failure")

// commitCatalog applies modification and writes the current catalog to datastore.
// It uses optimistic locking, if retry is set to true it will retry once on optimistic failure.
func (b *Bucket) commitCatalog(ctx context.Context, modification func(*Catalog) error) error {
	b.catalogLock.Lock()
	defer b.catalogLock.Unlock()
	if err := modification(&b.catalog); err != nil {
		return err
	}
	rawCatalog, err := json.Marshal(b.catalog)
	if err != nil {
		return err
	}
	result, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  &b.name,
		Key:     &b.catalog.Key,
		IfMatch: b.catalog.ETag,
		Body:    bytes.NewReader(rawCatalog),
	})
	if err != nil {
		sErr, ok := errors.AsType[smithy.APIError](err)
		if ok && sErr.ErrorCode() == "PreconditionFailed" {
			return ErrOptimisticLock
		}
		return err
	}
	b.catalog.ETag = result.ETag
	return nil
}
