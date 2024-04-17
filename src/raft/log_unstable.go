package raft

import (
	"log"
)

type unstable struct {
	snap   *Snapshot
	ent    []Entry
	offset int
}

func (u *unstable) firstIndex() int {
	if u.snap != nil {
		return u.snap.Metadata.Index + 1
	}
	return 0
}

func (u *unstable) snapshot() *Snapshot { return u.snap }

func (u *unstable) lastIndex() int {
	if n := len(u.ent); n != 0 {
		return u.offset + n - 1
	}
	if u.snap != nil {
		return u.snap.Metadata.Index
	}
	return 0
}

func (u *unstable) lastTerm() int { return u.term(u.lastIndex()) }

func (u *unstable) term(i int) int {
	if i < u.offset {
		if u.snap != nil && u.snap.Metadata.Index == i {
			return u.snap.Metadata.Term
		}
		return 0
	}

	if i > u.lastIndex() {
		return 0
	}

	return u.ent[i-u.offset].Term
}

func (u *unstable) stableTo(s Snapshot) {
	if s.Metadata.Index < u.lastIndex() {
		u.ent = u.entriesFrom(s.Metadata.Index + 1)
		u.offset = s.Metadata.Index + 1
		u.shrinkEntriesArray()
		u.snap = &s
	} else {
		u.restoreSnapshot(s)
	}
}

func (u *unstable) shrinkEntriesArray() {
	if n := len(u.ent); n*2 < cap(u.ent) {
		newEnt := make([]Entry, n)
		copy(newEnt, u.ent)
		u.ent = newEnt
	}
}

func (u *unstable) restoreEntries(entries []Entry) { u.ent = entries }

func (u *unstable) restoreSnapshot(s Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.ent = []Entry{}
	u.snap = &s
}

func (u *unstable) append(entries ...Entry) { u.ent = append(u.ent, entries...) }

func (u *unstable) truncateAndAppend(entries ...Entry) {
	if len(entries) == 0 {
		return
	}

	i := entries[0].Index
	switch {
	case i == u.offset+int(len(u.ent)):
		u.append(entries...)
	case i == u.offset:
		u.offset = i
		u.ent = entries
	default:
		u.ent = append(u.slice(u.offset, i), entries...)
	}
}

func (u *unstable) slice(l, r int) []Entry {
	u.mustCheckOutOfBounds(l, r)
	return u.ent[l-u.offset : r-u.offset]
}

func (u *unstable) entriesFrom(i int) []Entry { return u.slice(i, u.lastIndex()+1) }

func (u *unstable) entries() []Entry { return u.entriesFrom(u.firstIndex()) }

func (u *unstable) mustCheckOutOfBounds(l, r int) {
	if l > r {
		log.Panicf("invalid unstable.slice %d > %d", l, r)
	}
	upper := u.offset + int(len(u.ent))
	if l < u.offset || r > upper {
		log.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", l, r, u.offset, upper)
	}
}
