// package catalog contains the structure of the lakedb index catalog format.
// The catalog serves as metadata to avoid hive partitioning which is not atomic and expensive due to ListBucket requests.
// Abstracted into its own package to avoid top level bloat (rarely used by end users).
package catalog

type Catalog struct {
	Key     string           `json:"-"`
	ETag    *string          `json:"-"`
	Version string           `json:"version"` // version is always v1 for now
	Tables  map[string]Table `json:"tables"`
}

type Table struct {
	Shards []Shard `json:"shards"`
}

type Shard struct {
	Size   int    `json:"size"`
	Target string `json:"target"`
	// range statistics defined per column.
	Ranges map[string]Range `json:"ranges"`
}

type Range struct {
	Max any `json:"max"`
	Min any `json:"min"`
}
