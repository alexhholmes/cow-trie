package main

import (
	"cow-trie/pkg"
)

func main() {
	t, _ := pkg.NewTrieWithCapacityAndTTL[int](4, 10)
	t.Put("a", 1)
	t.Put("ab", 2)
	t.Put("abc", 3)
	t.Put("a", 1)
	t.Put("a", 2)
	t.Put("def", 2)
}
