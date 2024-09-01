package pkg

import (
	"fmt"
	"sync"
)

// Trie is a thread-safe implementation of a copy-on-write trie with a
// version history and time-to-live (ttl) expiration of old versions.
type Trie[V any] struct {
	// mu is a mutex to protect concurrent access to the current trie root and
	// all trie members except for versions.
	mu sync.Mutex

	// current is the root node of the current version. This should not be
	// written to directly. Instead, copy-on-write by creating a new root node
	// and updating "current" to point to it.
	current *node[V]

	// version is the current version number. This is incremented each time a
	// put operation is performed.
	version int

	// muVersions is a mutex to protect concurrent access to the versions slice.
	// This is only used if ttl is set to a value other than `0`. Locks occur
	// when a new version is created for a non-nil current and when expired
	// versions are removed by the cleanup routine.
	muVersions sync.Mutex

	// versions is a list of previous root nodes. The current root node is
	// stored in "current".
	versions []*node[V]

	// capacity is the maximum number of versions that can be stored. A value
	// of 0 means "unlimited".
	capacity int

	// ttl is the time-to-live for each non-current version. A value of 0 means
	// "unlimited". As soon as a current version is superseded, its ttl starts.
	ttl int
}

func NewTrie[V any]() *Trie[V] {
	trie, _ := NewTrieWithCapacityAndTTL[V](0, 0)
	return trie
}

func NewTrieWithCapacity[V any](capacity int) (*Trie[V], error) {
	return NewTrieWithCapacityAndTTL[V](capacity, 0)
}

func NewTrieWithTTL[V any](ttl int) (*Trie[V], error) {
	return NewTrieWithCapacityAndTTL[V](0, ttl)
}

func NewTrieWithCapacityAndTTL[V any](capacity, ttl int) (*Trie[V], error) {
	if ttl < 0 {
		return nil, fmt.Errorf("ttl must be greater than or equal to 0")
	}
	if capacity < 0 {
		return nil, fmt.Errorf("capacity must be greater than or equal to 0")
	}

	if capacity == 0 {
		return &Trie[V]{
			versions: make([]*node[V], 0),
			ttl:      ttl,
		}, nil
	}
	return &Trie[V]{
		versions: make([]*node[V], 0, capacity),
		capacity: capacity,
		ttl:      ttl,
	}, nil
}

// Get retrieves the value associated with the given key. If the key does not
// exist, the second return value, `ok`, will be false.
func (t *Trie[V]) Get(key string) (val V, version int, ok bool) {
	// Immediately catch a pointer of the current version since concurrent
	// writes may change `t.current`. This will also hold a reference to any
	// root nodes that are being expired. As soon as `follow` is set to a child
	// node, the root node can be garbage collected.
	follow := t.current

	if follow == nil {
		return val, 0, false
	}

	// Key that equals an empty string is stored in the root node
	if key == "" {
		if follow.hasValue {
			return follow.value, follow.version, true
		}
		return val, 0, false
	}

	// Traverse the trie to find the value, if it exists
	for _, c := range key {
		if _, ok = follow.children[string(c)]; !ok {
			return val, 0, false
		}

		// Parent node can be garbage collected is this is the last reference
		follow = follow.children[string(c)]
	}

	return follow.value, follow.version, true
}

// Put inserts a new key-value pair into the trie. If the key already exists,
// the value will be updated.
func (t *Trie[V]) Put(key string, value V) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Create a new root node, version should increment because a current root
	// could be made nil by a delete operation.
	t.version++
	root := &node[V]{
		children: nil,
		version:  t.version,
	}

	// Key that equals an empty string is stored in the root node
	if key == "" {
		t.current.hasValue = true
		t.current.value = value
	}

	if t.current == nil {
		t.current = root
	} else {
		t.versions = append(t.versions, t.current)
		t.current = root
	}
}

// Delete removes the key-value pair from the trie. If the key does not exist,
// this will be a no-op.
func (t *Trie[V]) Delete(key string) {
	// TODO implement me
	panic("implement me")
}

type node[V any] struct {
	// ttl is only used for root nodes
	ttl      int
	version  int
	children map[string]*node[V]
	hasValue bool
	value    V
}
