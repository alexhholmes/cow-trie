package main

import "sync"

type KeyValueStore[V any] interface {
	Get(key string) (V, bool)
	Put(key string, value V)
	Delete(key string)
	Len() int
}

var _ KeyValueStore[int] = &Trie[int]{}

// Trie is a thread-safe implementation of a copy-on-write trie with a
// version history and time-to-live (ttl) expiration of old versions.
type Trie[V any] struct {
	mu sync.Mutex
	// current is the root node of the current version. This should not be
	// written to directly. Instead, copy-on-write by creating a new root node
	// and updating "current" to point to it.
	current *node[V]
	// versions is a list of previous root nodes. The current root node is
	// stored in "current".
	versions []*node[V]
	// capacity is the maximum number of versions that can be stored. A value
	// of 0 means "unlimited".
	capacity int
	// fullSize is the total number of nodes across all versions. Note, that
	// because this data structure is copy-on-write, the actual total may be
	// less than the "size" of each individual version.
	fullSize int
	// minimumSize is the minimum number of nodes that must be kept despite a
	// ttl expiration. A value of 0 means all versions are eligible for ttl
	// expiration and subsequent deletion.
	minimumSize int
	// currentSize is the number of nodes in the current version.
	currentSize int
	// ttl is the time-to-live for each non-current version. A value of 0 means
	// "unlimited". As soon as a current version is superseded, its ttl starts.
	ttl int
}

func NewTrie[V any]() *Trie[V] {
	return &Trie[V]{versions: []*node[V]{}}
}

func NewTrieWithCapacity[V any](capacity int) *Trie[V] {
	return &Trie[V]{
		versions: make([]*node[V], 0, capacity),
		capacity: capacity,
	}
}

func (t *Trie[V]) Get(key string) (val V, ok bool) {
	// Immediately catch a pointer of the current version since concurrent
	// writes may change the current version.
	current := t.current

	if current == nil {
		return val, false
	}

	if _, ok = current.children[key]; !ok {
		return val, false
	}

	follow := current
	for _, c := range key {
		if _, ok = follow.children[string(c)]; !ok {
			return val, false
		}

		follow = follow.children[string(c)]
	}

	return follow.value, true
}

func (t *Trie[V]) Put(key string, value V) {
	if t.current == nil {
		t.current = &node[V]{children: make(map[string]*node[V])}
	}

	n := t.current
	for _, c := range key {
		if _, ok := n.children[string(c)]; !ok {
			n.children[string(c)] = &node[V]{children: make(map[string]*node[V])}
		}
	}

	// follow := t.current
	// for _, c := range key {
	// 	if _, ok := follow.children[string(c)]; !ok {
	// 		follow.children[string(c)] = &node[V]{children: make(map[string]*node[V])}
	// 	}
	//
	// 	follow = follow.children[string(c)]
	// }
	//
	// follow.value = value
	// t.size++
}

func (t *Trie[V]) Delete(key string) {
	// TODO implement me
	panic("implement me")
}

func (t *Trie[V]) Len() int {
	return t.size
}

type node[V any] struct {
	children map[string]*node[V]
	value    V
}

func main() {

}
