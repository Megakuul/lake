package lakedb

import (
	"reflect"
	"strings"
)

// getTableName extracts the table name.
// This is a standalone function because it is a contract between existing data and the reader.
func getTableName(table reflect.Value) string {
	return strings.ToLower(table.Type().Name())
}
