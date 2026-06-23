package lakedb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/megakuul/lakedb/catalog"
	"github.com/parquet-go/parquet-go"
)

func (b *Bucket) lookup(ctx context.Context, schema *parquet.Schema, ranges map[string]catalog.Range, checks map[string]func(parquet.Value) bool) ([]parquet.Row, error) {
	b.catalogLock.RLock()
	defer b.catalogLock.RUnlock()
	table, ok := b.catalog.Tables[schema.Name()]
	if !ok {
		return nil, fmt.Errorf("table '%s' does not exist", schema.Name())
	}

	rowGroups := []parquet.RowGroup{}
	for _, shard := range filterShards(table.Shards, ranges) {
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
		rowGroups = append(rowGroups, file.RowGroups()...)
	}
	rowGroup, err := parquet.MergeRowGroups(rowGroups, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to merge row groups: %v", err)
	}

	rowPositions := map[int64]struct{}{}

	for _, chunk := range rowGroup.ColumnChunks() {
		name := rowGroup.Schema().Columns()[chunk.Column()][0]
		chunkCheck := func(parquet.Value) bool { return true }
		if check, ok := checks[name]; ok {
			chunkCheck = check
		}
		matches, err := scanChunk(chunk, ranges[name], chunkCheck)
		if err != nil {
			return nil, fmt.Errorf("failed scan rows: %v", err)
		}
		if chunk.Column() == 0 {
			maps.Copy(rowPositions, matches)
			continue
		}
		// convert rows to a subset of matches (remove filtered out rows).
		for row := range rowPositions {
			if _, ok := matches[row]; ok {
				continue
			}
			delete(rowPositions, row)
		}
	}

	reader := rowGroup.Rows()
	defer reader.Close()

	rows := make([]parquet.Row, 0, len(rowPositions))
	for _, rowPosition := range slices.Sorted(maps.Keys(rowPositions)) {
		if err = reader.SeekToRow(rowPosition); err != nil {
			return nil, fmt.Errorf("failed to seek row: %v", err)
		}
		out := make([]parquet.Row, 1)
		_, err := reader.ReadRows(out)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read rows: %v", err)
		}
		rows = append(rows, out[0].Clone())
	}
	return rows, nil
}

// scanChunk checks the boundary for each page and applies the filter to each rows in matching pages.
// Returns a map containing the global row index for each matching row.
func scanChunk(chunk parquet.ColumnChunk, filterRange catalog.Range, check func(parquet.Value) bool) (map[int64]struct{}, error) {
	pages := chunk.Pages()
	defer pages.Close()

	approved := map[int64]struct{}{}

	columnIndex, err := chunk.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to read column index: %v", err)
	}
	offsetIndex, err := chunk.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to read column index: %v", err)
	}

	scannablePages := []int64{}
	for i := range columnIndex.NumPages() {
		switch filterMax := filterRange.Max.(type) {
		case int64:
			if columnIndex.MinValue(i).Kind() != parquet.Int64 || columnIndex.MinValue(i).Int64() > filterMax {
				continue
			}
		case float64:
			if columnIndex.MinValue(i).Kind() != parquet.Double || columnIndex.MinValue(i).Double() > filterMax {
				continue
			}
		case string:
			if columnIndex.MinValue(i).Kind() != parquet.ByteArray || string(columnIndex.MinValue(i).ByteArray()) > filterMax {
				continue
			}
		}
		switch filterMin := filterRange.Min.(type) {
		case int64:
			if columnIndex.MinValue(i).Kind() != parquet.Int64 || columnIndex.MaxValue(i).Int64() < filterMin {
				continue
			}
		case float64:
			if columnIndex.MinValue(i).Kind() != parquet.Double || columnIndex.MaxValue(i).Double() < filterMin {
				continue
			}
		case string:
			if columnIndex.MinValue(i).Kind() != parquet.ByteArray || string(columnIndex.MaxValue(i).ByteArray()) < filterMin {
				continue
			}
		}
		scannablePages = append(scannablePages, offsetIndex.FirstRowIndex(i))
	}

	for _, firstPageRow := range scannablePages {
		err := pages.SeekToRow(firstPageRow)
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
			if !check(value) {
				continue
			}
			approved[firstPageRow+int64(valueIdx)] = struct{}{}
		}
	}
	return approved, nil
}

// filterShards filters the shards based on the provided ranges (filter and shard range must overlap on every filter column to match).
func filterShards(shards []catalog.Shard, filter map[string]catalog.Range) []catalog.Shard {
	filteredShards := []catalog.Shard{}

Shards:
	for _, shard := range shards {
		for column, shardRange := range shard.Ranges {
			filterRange, ok := filter[column]
			if !ok {
				continue // unfiltered columns pass the filter
			}
			switch filterMax := filterRange.Max.(type) {
			case int64:
				if shardMin, ok := shardRange.Min.(int64); !ok || shardMin > filterMax {
					continue Shards
				}
			case float64:
				if shardMin, ok := shardRange.Min.(float64); !ok || shardMin > filterMax {
					continue Shards
				}
			case string:
				if shardMin, ok := shardRange.Min.(string); !ok || shardMin > filterMax {
					continue Shards
				}
			}
			switch filterMin := filterRange.Min.(type) {
			case int64:
				if shardMax, ok := shardRange.Max.(int64); !ok || shardMax < filterMin {
					continue Shards
				}
			case float64:
				if shardMax, ok := shardRange.Max.(float64); !ok || shardMax < filterMin {
					continue Shards
				}
			case string:
				if shardMax, ok := shardRange.Max.(string); !ok || shardMax < filterMin {
					continue Shards
				}
			}
		}
		filteredShards = append(filteredShards, shard)
	}
	return filteredShards
}
