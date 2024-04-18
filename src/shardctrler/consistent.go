package shardctrler

// Modified based on https://github.com/stathat/consistent/blob/master/consistent.go

import (
	"errors"
	"hash/crc32"
	"hash/fnv"
	"slices"
	"sort"
	"strconv"
)

// ErrEmptyCircle is the error returned when trying to get an element when nothing has been added to hash.
var ErrEmptyCircle = errors.New("empty circle")

// Consistent holds the information about the members of the consistent hash circle.
type Consistent struct {
	circle       map[uint32]string
	sortedHashes []uint32
	Replicas     int
	UseFnv       bool
}

// To change the number of replicas, set NumberOfReplicas before adding entries.
func NewConsistent() *Consistent {
	return &Consistent{
		circle:       make(map[uint32]string),
		Replicas:     100,
		sortedHashes: []uint32{},
		UseFnv:       false,
	}
}

// Add inserts a string element in the consistent hash.
func (c *Consistent) Add(elt string) {
	for i := 0; i < c.Replicas; i++ {
		c.circle[c.hashKey(c.eltKey(elt, i))] = elt
	}
	c.updateSortedHashes()
}

// Remove removes an element from the hash.
func (c *Consistent) Remove(elt string) {
	for i := 0; i < c.Replicas; i++ {
		delete(c.circle, c.hashKey(c.eltKey(elt, i)))
	}
	c.updateSortedHashes()
}

// Get returns an element close to where name hashes to in the circle.
func (c *Consistent) Get(name string) (string, error) {
	if len(c.circle) == 0 {
		return "", ErrEmptyCircle
	}
	key := c.hashKey(name)
	i := c.search(key)
	return c.circle[c.sortedHashes[i]], nil
}

// eltKey generates a string key for an element with an index.
func (c *Consistent) eltKey(elt string, idx int) string {
	// return elt + "|" + strconv.Itoa(idx)
	return strconv.Itoa(idx) + elt
}

func (c *Consistent) search(key uint32) int {
	i := sort.Search(len(c.sortedHashes), func(x int) bool { return c.sortedHashes[x] > key })
	if i < len(c.sortedHashes) {
		return i
	}
	return 0
}

func (c *Consistent) hashKey(key string) uint32 {
	if c.UseFnv {
		return c.hashKeyFnv(key)
	}
	return c.hashKeyCRC32(key)
}

func (c *Consistent) hashKeyCRC32(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) hashKeyFnv(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (c *Consistent) updateSortedHashes() {
	hashes := c.sortedHashes[:0]
	//reallocate if we're holding on to too much (1/4th)
	if cap(c.sortedHashes)/(c.Replicas*4) > len(c.circle) {
		hashes = nil
	}
	for k := range c.circle {
		hashes = append(hashes, k)
	}
	slices.Sort(hashes)
	c.sortedHashes = hashes
}
