// group implements utils to manage groups.
// A group is defined as a combination of all grouped by values in a single row (e.g. GROUP BY name,age means group is "linus,20").
// Because a group is not uniquely serializable (it can be a composite of multiple datatypes),
// this package implements a hashmap that maps such a group combination to a bitset of rows that are part of it.
package group

import (
	"hash/maphash"
	"iter"
	"math/big"

	"github.com/parquet-go/parquet-go"
)

var mapSeed = maphash.MakeSeed()

// Hashmap wraps a go Hashmap in a way that it works with []parquet.Value collisions (not == comparable).
type Hashmap struct {
	data map[uint64][]bucket
}

func NewHashmap() *Hashmap {
	return &Hashmap{
		data: map[uint64][]bucket{},
	}
}

// bucket serves as bucket structure for the hashmap.
type bucket struct {
	keys  []parquet.Value
	value *big.Int
}

func (b *bucket) check(keys []parquet.Value) bool {
	if len(b.keys) == 0 || len(b.keys) != len(keys) {
		return false
	}
	for i, key := range b.keys {
		if !parquet.Equal(key, keys[i]) {
			return false
		}
	}
	return true
}

// Get retrieves a value by hash key and the exact parquet value.
func (h *Hashmap) Get(keys []parquet.Value) (*big.Int, bool) {
	input := make([]byte, 0, 100)
	for _, key := range keys {
		input = key.AppendBytes(input)
	}
	hash := maphash.Bytes(mapSeed, input)

	if buckets, ok := h.data[hash]; ok {
		for _, bucket := range buckets {
			if bucket.check(keys) {
				return bucket.value, true
			}
		}
	}
	return new(big.Int), false
}

// Set upserts the value on the derived hash key / exact parquet value.
func (h *Hashmap) Set(keys []parquet.Value, value *big.Int) {
	input := make([]byte, 0, 100)
	for _, key := range keys {
		input = key.AppendBytes(input)
	}
	hash := maphash.Bytes(mapSeed, input)

	if buckets, ok := h.data[hash]; ok {
		for i, bucket := range buckets {
			if bucket.check(keys) {
				buckets[i].value = value
				return // duplicate; just ignore
			}
		}
		h.data[hash] = append(h.data[hash], bucket{keys: keys, value: value})
	} else {
		h.data[hash] = []bucket{{keys: keys, value: value}}
	}
}

// All returns an iterator for all values of the hashmap.
func (h *Hashmap) All() iter.Seq2[[]parquet.Value, *big.Int] {
	return func(yield func([]parquet.Value, *big.Int) bool) {
		for _, buckets := range h.data {
			for _, bucket := range buckets {
				if !yield(bucket.keys, bucket.value) {
					return
				}
			}
		}
	}
}
