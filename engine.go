package lake

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/big"
	"time"

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
	limit       int // if set to -1 there is no limit
	grouping    map[string]func(parquet.Value) (string, parquet.Value)
	aggregators map[string]func([]parquet.Value) parquet.Value
}

type chunkKey struct {
	hash  string
	exact parquet.Value
}

type row struct {
	group  []chunkKey
	values []parquet.Value
}

// lookup uses the provided ranges and checks to efficiently find all matching rows.
func (b *Bucket) aggregate(ctx context.Context, schema *parquet.Schema, q *query) ([]parquet.Row, error) {
	start := time.Now()
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

	println("catalog, merge and from disk: ", fmt.Sprint(time.Since(start)))
	start = time.Now()

	filterColumns, err := createFilterColumns(rowGroup.Schema(), q)
	if err != nil {
		return nil, err
	}

	// rows is a bitset compressed '[]bool{}' that maps row positions to their "match" status.
	var rows *big.Int

	for i, filterColumn := range filterColumns {
		println(filterColumn.name)
		chunk := rowGroup.ColumnChunks()[filterColumn.index]
		var matches big.Int
		if err := scanChunk(chunk, &matches, rows, filterColumn.chunkRange, filterColumn.chunkCheck); err != nil {
			return nil, fmt.Errorf("failed scan chunk: %v", err)
		}
		// first column captures all matches
		if i == 0 {
			rows = &matches
			continue
		}
		// other columns just remove bits from rows
		for row := range rows.BitLen() {
			if rows.Bit(row) == 1 && matches.Bit(row) == 0 {
				rows.SetBit(rows, row, 0)
			}
		}
	}

	println("grouping and filtering: ", fmt.Sprint(time.Since(start)))
	start = time.Now()

	count := 0
	for row := range rows.BitLen() {
		if rows.Bit(row) == 0 {
			continue
		}
		count++
	}

	println("total: ", count)

	// enforce limit
	// if q.limit > 0 {
	// 	rows = rows[:q.limit]
	// }

	println("limiting: ", fmt.Sprint(time.Since(start)))
	start = time.Now()

	result := make([]parquet.Row, 0)
	return result, nil
	//
	// groups := newHashmap[[][]parquet.Value]()
	// for row, ok := range rows {
	// 	if ok {
	// 		continue
	// 	}
	// 	groupingHash, groupingChain := strings.Builder{}, []parquet.Value{}
	// 	for _, chunkKey := range rows[i].group {
	// 		groupingHash.WriteString(chunkKey.hash)
	// 		groupingChain = append(groupingChain, chunkKey.exact)
	// 	}
	// 	columns, ok := groups.get(groupingHash.String(), groupingChain)
	// 	if !ok {
	// 		columns = make([][]parquet.Value, len(rowGroup.Schema().Columns()))
	// 	}
	// 	for _, value := range row.values {
	// 		columns[value.Column()] = append(columns[value.Column()], value)
	// 	}
	// 	groups.set(groupingHash.String(), groupingChain, columns)
	// }
	// for hash, keyChain := range groups.keys() {
	// 	columns, ok := groups.get(hash, keyChain)
	// 	if !ok {
	// 		continue
	// 	}
	// 	aggregated := make(parquet.Row, 0, len(columns))
	// 	for column, values := range columns {
	// 		columnName := rowGroup.Schema().Columns()[column][0]
	// 		aggregate, ok := q.aggregators[columnName]
	// 		if !ok {
	// 			if group, ok := q.grouping[columnName]; ok {
	// 				_, derived := group(values[0])
	// 				aggregated = append(aggregated, derived)
	// 			} else {
	// 				aggregated = append(aggregated, parquet.NullValue())
	// 			}
	// 			continue
	// 		}
	// 		aggregated = append(aggregated, aggregate(values))
	// 	}
	// 	result = append(result, aggregated)
	// }
	//
	// println("aggregating: ", fmt.Sprint(time.Since(start)))
	// start = time.Now()
	// return result, nil
}

func retrieve() {
	// reader := rowGroup.Rows()
	// defer reader.Close()
	//
	// result := make([]parquet.Row, 0)
	// for i, row := range rows {
	// 	if len(row.values) < 1 {
	// 		continue
	// 	}
	// 	if err = reader.SeekToRow(int64(i)); err != nil {
	// 		return nil, fmt.Errorf("failed to seek row: %v", err)
	// 	}
	// 	buffer := make([]parquet.Row, 1)
	// 	_, err := reader.ReadRows(buffer)
	// 	if err != nil && err != io.EOF {
	// 		return nil, fmt.Errorf("failed to read rows: %v", err)
	// 	}
	// 	result = append(result, buffer...)
	// }
	// return result, nil
}

// scanChunk checks the boundary for each page and applies the filter to each row in matching pages.
// It marks all passing rows in the provided rows map as true.
func scanChunk(chunk parquet.ColumnChunk, matches, skip *big.Int, filterRange catalog.Range, check func(parquet.Value) bool) error {
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
			if columnIndex.MaxValue(i).Kind() != parquet.Int64 || columnIndex.MaxValue(i).Int64() < filterMin {
				continue
			}
		case float64:
			if columnIndex.MaxValue(i).Kind() != parquet.Double || columnIndex.MaxValue(i).Double() < filterMin {
				continue
			}
		case string:
			if columnIndex.MaxValue(i).Kind() != parquet.ByteArray || string(columnIndex.MaxValue(i).ByteArray()) < filterMin {
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
			if skip != nil && skip.Bit(int(firstPageRow)+valueIdx) == 0 {
				continue
			}
			if check != nil && !check(value) {
				continue
			}
			matches.SetBit(matches, int(firstPageRow)+valueIdx, 1)
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
