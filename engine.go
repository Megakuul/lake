package lake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/megakuul/lakedb/catalog"
	"github.com/parquet-go/parquet-go"
)

// query is the internal api between the engine and the querybuilder.
// it defines all query stages:
// 1. range filters (compares numeral or alphabetical ranges against the catalog / parquet statistics)
// 2. check filters (perform exact fine grained filtering on values that passed the range filter).
// 3. limit applies to stop the filtering process.
// 4. grouping (uses fine grained filters to group rows into one or more "windows" (by default just one global window))
// 5. aggregators (takes the grouped "windows" and applies aggregation to each column to collapse the grouped rows)
type query struct {
	ranges      map[string]catalog.Range
	checks      map[string]func(parquet.Value) bool
	limit       int                                              // if set to -1 there is no limit
	grouping    []map[string]func(parquet.Value) bool            // grouping must contain at least one entry (otherwise nothing is returned).
	aggregators []map[string]func([]parquet.Value) parquet.Value // aggregators is expected to match to the number of groups.
}

// lookup uses the provided ranges and checks to efficiently find all matching rows.
func (b *Bucket) lookup(ctx context.Context, schema *parquet.Schema, q *query) ([][]parquet.Row, error) {
	b.catalogLock.RLock()
	defer b.catalogLock.RUnlock()
	table, ok := b.catalog.Tables[schema.Name()]
	if !ok {
		return nil, fmt.Errorf("table '%s' does not exist", schema.Name())
	}

	rowGroups := []parquet.RowGroup{}
	for _, shard := range filterShards(table.Shards, q.ranges) {
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

	// TODO this could be a bitset instead to reduce size from chunk.NumValues() * 1 byte -> chunk.NumValues() * 1 bit
	// but linus, why is this not a map anymore?
	// -> It's a tragedy I know, even the cpu cache misses the map ^^
	rows := make([]bool, rowGroup.NumRows())

	for _, chunk := range rowGroup.ColumnChunks() {
		name := rowGroup.Schema().Columns()[chunk.Column()][0]
		chunkCheck := func(parquet.Value) bool { return true }
		if check, ok := q.checks[name]; ok {
			chunkCheck = check
		}
		matches := make([]bool, rowGroup.NumRows())
		if err := scanChunk(chunk, matches, q.ranges[name], chunkCheck); err != nil {
			return nil, fmt.Errorf("failed scan chunk: %v", err)
		}
		// take the set from the first column as base.
		if chunk.Column() == 0 {
			rows = matches
			continue
		}
		// subsequent matches will just remove non-matching values from base (column filters are always AND joined).
		for row, ok := range rows {
			if ok && matches[row] {
				continue
			}
			rows[row] = false
		}
	}

	if len(q.aggregators) > 0 {
		groups := make([]map[string][]parquet.Value, len(q.grouping))
		for group := range groups {
			groups[group] = map[string][]parquet.Value{}
		}
		for _, chunk := range rowGroup.ColumnChunks() {
			name := rowGroup.Schema().Columns()[chunk.Column()][0]

			// pure optimization; checks if there is no planned aggregation for this chunk.
			// if not, it immediately skips the full chunk.
			skip := true
			for _, aggregators := range q.aggregators {
				if _, ok := aggregators[name]; ok {
					skip = false
				}
			}
			if skip {
				continue
			}

			pager := chunk.Pages()
			defer pager.Close()
			for row := 0; row < len(rows); row++ {
				if !rows[row] {
					continue
				}
				if err = pager.SeekToRow(int64(row)); err != nil {
					return nil, fmt.Errorf("failed to seek to row: %v", err)
				}
				page, err := pager.ReadPage()
				if err != nil {
					return nil, fmt.Errorf("failed to read page: %v", err)
				}
				batch := 0
				for i := range page.NumRows() {
					batch++
					if !rows[int64(row)+i] {
						break
					}
				}
				row += batch - 1
				buffer := make([]parquet.Value, batch)
				n, err := page.Values().ReadValues(buffer)
				if err != nil && !errors.Is(err, io.EOF) {
					return nil, fmt.Errorf("failed to read page values: %v", err)
				}
				for _, value := range buffer[:n] {
					for group, groupFilters := range q.grouping {
						if groupFilter, ok := groupFilters[name]; !ok || groupFilter(value) {
							groups[group][name] = append(groups[group][name], value)
						}
					}
				}
			}
		}

		result := make([][]parquet.Row, len(q.grouping))
		for group, columns := range groups {
			aggregatedRow := make(parquet.Row, 0, len(rowGroup.Schema().Columns()))
			for _, column := range rowGroup.Schema().Columns() {
				values := columns[column[0]]
				aggregate, ok := q.aggregators[group][column[0]]
				if ok {
					aggregatedRow = append(aggregatedRow, aggregate(values))
				} else {
					aggregatedRow = append(aggregatedRow, parquet.NullValue())
				}
			}
			result[group] = []parquet.Row{aggregatedRow}
		}

		return result, nil
	}

	reader := rowGroup.Rows()
	defer reader.Close()

	result := make([][]parquet.Row, len(q.grouping))
	for row, ok := range rows {
		if !ok {
			continue
		}
		if err = reader.SeekToRow(int64(row)); err != nil {
			return nil, fmt.Errorf("failed to seek row: %v", err)
		}
		rowBuffer := make([]parquet.Row, 1)
		_, err := reader.ReadRows(rowBuffer)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read rows: %v", err)
		}
		result[0] = append(result[0], rowBuffer...)
	}
	return result, nil
}

// scanChunk checks the boundary for each page and applies the filter to each row in matching pages.
// It marks all passing rows in the provided rows map as true.
func scanChunk(chunk parquet.ColumnChunk, rows []bool, filterRange catalog.Range, check func(parquet.Value) bool) error {
	pages := chunk.Pages()
	defer pages.Close()

	columnIndex, err := chunk.ColumnIndex()
	if err != nil {
		return fmt.Errorf("failed to read column index: %v", err)
	}
	offsetIndex, err := chunk.OffsetIndex()
	if err != nil {
		return fmt.Errorf("failed to read column index: %v", err)
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
			return fmt.Errorf("failed to seek to page row: %v", err)
		}
		page, err := pages.ReadPage()
		if err != nil {
			return fmt.Errorf("failed to read page: %v", err)
		}

		values := make([]parquet.Value, page.NumValues())
		n, err := page.Values().ReadValues(values)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read rows: %v", err)
		}
		for valueIdx, value := range values[:n] {
			if !check(value) {
				continue
			}
			if int(firstPageRow)+valueIdx >= len(rows) {
				return fmt.Errorf("more rows then values in column chunk this is not allowed by lakedb!")
			}
			rows[firstPageRow+int64(valueIdx)] = true
		}
	}
	return nil
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
