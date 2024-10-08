package pkg

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
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
	current *root[V]

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
	versions []*root[V]

	// capacity is the maximum number of versions that can be stored. A value
	// of 0 means "unlimited".
	capacity int

	// ttl is the time-to-live for each non-current version. A value of 0 means
	// "unlimited". As soon as a current version is superseded, its ttl starts.
	ttl int
}

// root nodes are the top-level nodes in the trie and contain a time-to-live
// value for when they are stored in the versions list.
type root[V any] struct {
	ttl int
	node[V]
}

type node[V any] struct {
	version  int
	children map[string]*node[V]
	hasValue bool
	value    V
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

	t := &Trie[V]{
		versions: []*root[V]{},
		ttl:      ttl,
	}
	if capacity > 0 {
		t.versions = make([]*root[V], 0, capacity)
		t.capacity = capacity
	}

	if ttl > 0 {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, os.Kill, syscall.SIGTERM)

		sleepTime := time.Duration(t.ttl) * time.Second / 10
		if sleepTime < time.Second {
			sleepTime = time.Second
		}

		// Cleanup routine for expired versions
		go func(t *Trie[V]) {
			for {
				select {
				case <-sigChan:
					// OS signal received, exit the cleanup routine
					return
				default:
					// Perform cleanup of expired versions roots
					t.muVersions.Lock()
					count := 0
					for _, v := range t.versions {
						if v.ttl < time.Now().Second() {
							// Expired version, add to count and continue
							count++
						} else {
							break
						}
					}

					if count > 0 {
						// Remove expired versions
						t.versions = t.versions[count:]
					}
					t.muVersions.Unlock()
				}

				time.Sleep(sleepTime)
			}
		}(t)
	}

	return t, nil
}

// Get retrieves the value associated with the given key. If the key does not
// exist, the second return value, `ok`, will be false.
func (t *Trie[V]) Get(key string) (val V, version int, ok bool) {
	// Immediately catch a pointer of the current version since concurrent
	// writes may change `t.current`. This will also hold a reference to any
	// root nodes that are being expired. As soon as `follow` is set to a child
	// node, the root node can be garbage collected.
	follow := &t.current.node

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

	// We want to return the version number of the root node we access from
	version = follow.version

	// Traverse the trie to find the value, if it exists
	for _, c := range key {
		if _, ok = follow.children[string(c)]; !ok {
			return val, 0, false
		}

		// Parent node can be garbage collected if this is the last reference
		follow = follow.children[string(c)]
	}

	if follow.hasValue {
		return follow.value, version, true
	}
	return val, 0, false
}

func (t *Trie[V]) GetVersion(key string, version int) (val V, ok bool) {
	if version > t.version || version < 0 {
		return val, false
	}

	t.mu.Lock()
	if version == t.version {
		defer t.mu.Unlock()
		val, _, ok = t.Get(key)
		return val, ok
	}
	t.mu.Unlock()

	t.muVersions.Lock()
	defer t.muVersions.Unlock()

	if version != t.version && len(t.versions) == 0 {
		return val, false
	} else if t.versions[0].version > version {
		return val, false
	}

	index := version - t.versions[0].version
	if index >= len(t.versions) {
		return val, false
	}
	follow := &t.versions[index].node

	// Found the version, get the value as a node type
	if key == "" {
		if follow.hasValue {
			return follow.value, true
		}
		return val, false
	}

	for _, c := range key {
		if _, ok = follow.children[string(c)]; !ok {
			return val, false
		}
		follow = follow.children[string(c)]
	}

	if follow.hasValue {
		return follow.value, true
	}
	return val, false
}

// Put inserts a new key-value pair into the trie. If the key already exists,
// the value will be updated.
func (t *Trie[V]) Put(key string, value V) {
	t.mu.Lock()
	// TODO optimize this by adding locks onto the nodes
	defer t.mu.Unlock()

	// Create a new root node, version should increment because a current root
	// could be made nil by a delete operation.
	t.version++
	r := &root[V]{
		node: node[V]{
			children: nil,
			version:  t.version,
		},
	}

	// Key that equals an empty string is stored in the root node
	if key == "" {
		r.hasValue = true
		r.value = value
	}

	if t.current == nil {
		t.current = r
	} else {
		// Copy the current root node to the new root node
		r.children = make(map[string]*node[V], len(t.current.children))
		for k, v := range t.current.children {
			r.children[k] = v
		}

		// Add the current root node to the versions list with ttl
		t.muVersions.Lock()
		if t.ttl > 0 {
			t.current.ttl = time.Now().Second() + t.ttl
		}
		if t.capacity > 0 && len(t.versions) >= t.capacity {
			// Full capacity, evict the oldest version
			t.versions = append(t.versions[1:], t.current)
		} else {
			t.versions = append(t.versions, t.current)
		}
		t.muVersions.Unlock()

		// And update the current root node to the new root node
		t.current = r

		// Put the key-value pair into the trie (unless it was an empty string).
		follow := &r.node
		for _, c := range key {
			if _, ok := follow.children[string(c)]; !ok {
				// Create a new node if the child does not exist, no
				// copy-on-write is needed.
				follow.children[string(c)] = &node[V]{
					children: make(map[string]*node[V]),
					version:  t.version,
				}
				follow = follow.children[string(c)]
			} else {
				// Copy-on-write is needed, create a new node and copy the
				// child nodes.
				newNode := &node[V]{
					children: make(map[string]*node[V], len(follow.children[string(c)].children)),
					version:  t.version,
				}
				for k, v := range follow.children[string(c)].children {
					newNode.children[k] = v
				}
				follow.children[string(c)] = newNode
				follow = newNode
			}
		}
		follow.hasValue = true
		follow.value = value
	}
}

// Delete removes the key-value pair from the trie. If the key does not exist,
// this will be a no-op.
func (t *Trie[V]) Delete(key string) {
	if t.current == nil || key == "" && !t.current.hasValue {
		// Skip early for an empty string so mutex lock isn't held
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Stack to hold the nodes that need to be copied-on-write
	var stack []*node[V]

	// Traverse the trie to check if the key exists
	follow := &t.current.node
	for _, c := range key {
		if _, ok := follow.children[string(c)]; !ok {
			// Key does not exist, return early
			return
		}
		stack = append(stack, follow)
		follow = follow.children[string(c)]
	}

	if !follow.hasValue {
		// Value does not exist in the node that matches key, return early
		return
	}

	// Copy-on-write the nodes that are in the stack
	t.version++
	r := &root[V]{
		node: node[V]{
			children: make(map[string]*node[V], len(t.current.children)),
			version:  t.version,
		},
	}

	// Copy the current root node to the new root node
	for k, v := range t.current.children {
		r.children[k] = v
	}

	parent := &r.node
	prev := &r.node
	for i, n := range stack {
		// Copy-on-write is needed, create a new node and copy the child nodes.
		newNode := &node[V]{
			children: make(map[string]*node[V], len(n.children)),
			version:  t.version,
		}
		for k, v := range n.children {
			newNode.children[k] = v
		}
		parent.children[string(key[i])] = newNode
		prev = parent
		parent = newNode
	}

	// Remove the value from the node
	parent.hasValue = false
	if len(parent.children) == 0 {
		// Remove the node if it has no children
		delete(prev.children, string(key[len(key)-1]))
	}

	// Add the current root node to the versions list with ttl
	t.muVersions.Lock()
	if t.ttl > 0 {
		t.current.ttl = time.Now().Second() + t.ttl
	}
	if t.capacity > 0 && len(t.versions) >= t.capacity {
		// Full capacity, evict the oldest version
		t.versions = append(t.versions[1:], t.current)
	} else {
		t.versions = append(t.versions, t.current)
	}

	t.current = r
	t.muVersions.Unlock()
}

func (t *Trie[V]) Version() int {
	return t.version
}
