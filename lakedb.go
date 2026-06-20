package lakedb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"path"
	"slices"
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
	"github.com/parquet-go/parquet-go/format"
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

func lookup[T any](ctx context.Context, b *Bucket, tableName string, bounds Boundaries, filters map[string]checkFilter) ([]T, error) {
	b.catalogLock.RLock()
	defer b.catalogLock.RUnlock()
	table, ok := b.catalog.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	outputRows := []T{}
	for _, shard := range filterShards(table.Shards, bounds) {
		result, err := b.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &b.name,
			Key:    &shard.Target,
		})
		if err != nil {
			return nil, err
		}
		defer result.Body.Close()
		buffer, err := io.ReadAll(result.Body)
		if err != nil {
			return nil, err
		}
		file, err := parquet.OpenFile(bytes.NewReader(buffer), int64(shard.Size))
		if err != nil {
			return nil, fmt.Errorf("cannot open shard file '%s': %v", shard.Target, err)
		}
		if len(file.OffsetIndexes()) != len(file.ColumnIndexes()) {
			return nil, fmt.Errorf("corrupted column or offset index")
		}

		rows := map[int64]struct{}{}
		for rgIdx, rg := range file.RowGroups() {
			for columnIdx, column := range file.Root().Columns() {
				// columnIndexes are stored in this weird rg1.col1,rg1.col2,rg2.col1,rg2.col2 format
				// the following code simply unwraps this. To avoid panics on corrupted data we also check boundary.
				columnIndexIdx := (rgIdx * len(file.Root().Columns())) + columnIdx
				if len(file.ColumnIndexes()) <= columnIndexIdx {
					return nil, fmt.Errorf("corrupted column index")
				}
				columnIndex := file.ColumnIndexes()[columnIndexIdx]
				offsetIndex := file.OffsetIndexes()[columnIndexIdx]

				if len(rg.ColumnChunks()) <= columnIdx {
					return nil, fmt.Errorf("corrupted column chunk in row group")
				}
				chunk := rg.ColumnChunks()[columnIdx]

				checkFilter, ok := filters[column.Name()]
				if !ok {
					checkFilter = func(parquet.Value) bool { return true }
				}
				checkBoundary := func(min, max []byte) bool { return true }
				switch column.Type() {
				case parquet.Int64Type:
					boundary := bounds.Ints[column.Name()]
					checkBoundary = func(rawMin, rawMax []byte) bool {
						min := int64(binary.LittleEndian.Uint64(rawMin))
						max := int64(binary.LittleEndian.Uint64(rawMax))
						return checkNumeralBoundary(min, max, boundary.Min, boundary.Max)
					}
				case parquet.DoubleType:
					boundary := bounds.Doubles[column.Name()]
					checkBoundary = func(rawMin, rawMax []byte) bool {
						min := math.Float64frombits(binary.LittleEndian.Uint64(rawMin))
						max := math.Float64frombits(binary.LittleEndian.Uint64(rawMax))
						return checkNumeralBoundary(min, max, boundary.Min, boundary.Max)
					}
				}

				matches, err := scanRows(chunk, columnIndex, offsetIndex, checkBoundary, checkFilter)
				if err != nil {
					return nil, fmt.Errorf("failed scan rows: %v", err)
				}
				if columnIdx == 0 {
					maps.Copy(rows, matches)
					continue
				}
				// convert rows to a subset of matches (remove filtered out rows).
				for row := range rows {
					if _, ok := matches[row]; ok {
						continue
					}
					delete(rows, row)
				}
			}
		}

		reader := parquet.NewGenericReader[T](file)
		defer reader.Close()

		rowsBuffer := make([]T, 1)
		for _, row := range slices.Sorted(maps.Keys(rows)) {
			reader.SeekToRow(row)
			n, err := reader.Read(rowsBuffer)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("failed to read rows: %v", err)
			}
			outputRows = append(outputRows, rowsBuffer[:n]...)
		}
	}
	return outputRows, nil
}

type (
	checkFilter   func(parquet.Value) bool
	checkBoundary func(minRaw []byte, maxRaw []byte) bool
)

// scanRows checks the boundary for each page and applies the filter to each rows in matching pages.
// Returns a map containing the global row index for each matching row.
func scanRows(chunk parquet.ColumnChunk, column format.ColumnIndex, offset format.OffsetIndex, checkBoundary checkBoundary, checkFilter checkFilter) (map[int64]struct{}, error) {
	pages := chunk.Pages()
	defer pages.Close()

	approved := map[int64]struct{}{}
	for pageIdx, rawMin := range column.MinValues {
		if len(column.MaxValues) <= pageIdx {
			return nil, fmt.Errorf("corrupted column index boundary statistic")
		}
		if !checkBoundary(rawMin, column.MaxValues[pageIdx]) {
			continue
		}
		if len(offset.PageLocations) <= pageIdx {
			return nil, fmt.Errorf("corrupted offset index")
		}
		pageLocation := offset.PageLocations[pageIdx]

		err := pages.SeekToRow(pageLocation.FirstRowIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to page row: %v", err)
		}
		page, err := pages.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("failed to read page: %v", err)
		}
		values := make([]parquet.Value, page.NumValues())
		n, err := page.Values().ReadValues(values)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read rows: %v", err)
		}
		for valueIdx, value := range values[:n] {
			if checkFilter(value) {
				approved[pageLocation.FirstRowIndex+int64(valueIdx)] = struct{}{}
			}
		}
	}
	return approved, nil
}

// filterShards filters the shards based on the provided bounds.
func filterShards(shards []Shard, bounds Boundaries) []Shard {
	filteredShards := []Shard{}
	for _, shard := range shards {
		for name, field := range shard.Boundaries.Ints {
			fieldFilter, ok := bounds.Ints[name]
			if ok && field.Min != nil && field.Max != nil {
				if !checkNumeralBoundary(*field.Min, *field.Max, fieldFilter.Min, fieldFilter.Max) {
					break
				}
			}
		}
		for name, field := range shard.Boundaries.Doubles {
			fieldFilter, ok := bounds.Doubles[name]
			if ok && field.Min != nil && field.Max != nil {
				if !checkNumeralBoundary(*field.Min, *field.Max, fieldFilter.Min, fieldFilter.Max) {
					break
				}
			}
		}
		filteredShards = append(filteredShards, shard)
	}
	return filteredShards
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
