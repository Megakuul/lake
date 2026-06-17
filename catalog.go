package lakedb

type Catalog struct {
	Key    string           `json:"-"`
	ETag   *string          `json:"-"`
	Tables map[string]Table `json:"tables"`
}

type Table struct {
	Shards []Shard `json:"shards"`
}

type Shard struct {
	Size       int        `json:"size"`
	Target     string     `json:"target"`
	Boundaries Boundaries `json:"boundaries"`
}

type Boundaries struct {
	Ints    map[string]IntBoundary    `json:"ints"`
	Doubles map[string]DoubleBoundary `json:"doubles"`
}

func newBoundaries() Boundaries {
	return Boundaries{
		Ints:    map[string]IntBoundary{},
		Doubles: map[string]DoubleBoundary{},
	}
}

type IntBoundary struct {
	Max *int64 `json:"max"`
	Min *int64 `json:"min"`
}

type DoubleBoundary struct {
	Max *float64 `json:"max"`
	Min *float64 `json:"min"`
}
