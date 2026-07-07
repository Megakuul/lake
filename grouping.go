package lake

import (
	"bytes"
	"math"
	"time"

	"github.com/parquet-go/parquet-go"
)

type Grouper func(parquet.Value) parquet.Value

// Exact performs an exact distinct grouping on the column.
// Every unique value creates a new column group.
func Exact() Grouper {
	return func(v parquet.Value) parquet.Value {
		return v
	}
}

// Round is only for numerical values (lake.Int / lake.Float).
// It rounds the value to the bucket size to retrieve the group value.
func Round(bucketSize int64) Grouper {
	return func(v parquet.Value) parquet.Value {
		switch v.Kind() {
		case parquet.Int64:
			return parquet.Int64Value(int64(math.Round(float64(v.Int64())/float64(bucketSize)) * float64(bucketSize)))
		case parquet.Double:
			return parquet.DoubleValue(math.Round(v.Double()/float64(bucketSize)) * float64(bucketSize))
		}
		return v
	}
}

// Floor is only for numerical values (lake.Int / lake.Float).
// It floors the value to the bucket size to retrieve the group value.
func Floor(bucketSize int64) Grouper {
	return func(v parquet.Value) parquet.Value {
		switch v.Kind() {
		case parquet.Int64:
			return parquet.Int64Value(int64(math.Floor(float64(v.Int64())/float64(bucketSize))) * bucketSize)
		case parquet.Double:
			return parquet.DoubleValue(math.Floor(v.Double()/float64(bucketSize)) * float64(bucketSize))
		}
		return v
	}
}

// Fold is only for string values (lake.String).
// It folds the value into lowercase to retrieve the group value.
func Fold() Grouper {
	return func(v parquet.Value) parquet.Value {
		if v.Kind() != parquet.ByteArray {
			return v
		}
		return parquet.ByteArrayValue(bytes.ToLower(v.ByteArray()))
	}
}

// Prefix is only for string values (lake.String).
// It trims everything before :length from the value and uses this as group value.
func Prefix(length int) Grouper {
	return func(v parquet.Value) parquet.Value {
		if v.Kind() != parquet.ByteArray {
			return v
		}
		data := v.ByteArray()
		if len(data) < length {
			length = len(data)
		}
		return parquet.ByteArrayValue(data[:length])
	}
}

// Suffix is only for string values (lake.String).
// It trims everything after len(data)-length: from the value and uses this as group value.
func Suffix(length int) Grouper {
	return func(v parquet.Value) parquet.Value {
		if v.Kind() != parquet.ByteArray {
			return v
		}
		data := v.ByteArray()
		suffixOffset := len(data) - length
		if len(data)-length < 0 {
			suffixOffset = 0
		}
		return parquet.ByteArrayValue(data[suffixOffset:])
	}
}

type DateRange int

const (
	DateMinute DateRange = iota
	DateHour
	DateDay
	DateMonth
	DateYear
)

// Date is only for lake.Int values that represent a time.Time in Unix (seconds).
// It removes everything from the date after the range to group values to "day", "hour", etc. buckets.
func Date(r DateRange) Grouper {
	return func(v parquet.Value) (result parquet.Value) {
		if v.Kind() != parquet.Int64 {
			return v
		}
		date := time.Unix(v.Int64(), 0).UTC()
		switch r {
		case DateMinute:
			result = parquet.Int64Value(int64(time.Date(
				date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), 0, 0, time.UTC,
			).Unix()))
		case DateHour:
			result = parquet.Int64Value(int64(time.Date(
				date.Year(), date.Month(), date.Day(), date.Hour(), 0, 0, 0, time.UTC,
			).Unix()))
		case DateDay:
			result = parquet.Int64Value(int64(time.Date(
				date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC,
			).Unix()))
		case DateMonth:
			result = parquet.Int64Value(int64(time.Date(
				date.Year(), date.Month(), 1, 0, 0, 0, 0, time.UTC,
			).Unix()))
		case DateYear:
			result = parquet.Int64Value(int64(time.Date(
				date.Year(), 1, 1, 0, 0, 0, 0, time.UTC,
			).Unix()))
		}
		return result.Level(v.RepetitionLevel(), v.DefinitionLevel(), v.Column())
	}
}
