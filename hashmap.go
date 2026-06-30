package lake

import (
	"iter"

	"github.com/parquet-go/parquet-go"
)

// hashmap wraps a go hashmap in a way that it works with parquet.Value collisions (not == comparable).
// The reason for this is that the builtin go map uses '==' equality checks for bucket collision traversal (unsupported by parquet.Value).
type hashmap[T any] struct {
	data map[string][]hashmapBucket[T]
}

func newHashmap[T any]() *hashmap[T] {
	return &hashmap[T]{
		data: map[string][]hashmapBucket[T]{},
	}
}

// hashmapBucket serves as bucket structure for the hashmap.
type hashmapBucket[T any] struct {
	keyChain []parquet.Value
	value    T
}

// get retrieves a value by derived hash key and the exact parquet value.
func (h *hashmap[T]) get(derived string, keyChain []parquet.Value) (value T, ok bool) {
	if buckets, ok := h.data[derived]; ok {
		for _, bucket := range buckets {
			if h.checkBucket(bucket, keyChain) {
				return bucket.value, true
			}
		}
	}
	return value, false
}

func (h *hashmap[T]) checkBucket(bucket hashmapBucket[T], keyChain []parquet.Value) bool {
	if len(bucket.keyChain) == 0 || len(bucket.keyChain) != len(keyChain) {
		return false
	}
	for i, key := range bucket.keyChain {
		if !parquet.Equal(key, keyChain[i]) {
			return false
		}
	}
	return true
}

// set upserts the value on the derived hash key / exact parquet value.
func (h *hashmap[T]) set(derived string, keyChain []parquet.Value, value T) {
	if buckets, ok := h.data[derived]; ok {
		for i, bucket := range buckets {
			if h.checkBucket(bucket, keyChain) {
				buckets[i].value = value
				return // duplicate; just ignore
			}
		}
		h.data[derived] = append(h.data[derived], hashmapBucket[T]{keyChain: keyChain, value: value})
	} else {
		h.data[derived] = []hashmapBucket[T]{{keyChain: keyChain, value: value}}
	}
}

// keys returns an iterator for all keys of the hashmap.
func (h *hashmap[T]) keys() iter.Seq2[string, []parquet.Value] {
	return func(yield func(string, []parquet.Value) bool) {
		for key, buckets := range h.data {
			for _, bucket := range buckets {
				if !yield(key, bucket.keyChain) {
					return
				}
			}
		}
	}
}
