package raft

import (
	"log"
	"slices"
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
	if l := len(u.ent); l != 0 {
		return u.offset + int(l) - 1
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
	if s.Metadata.Index < u.offset {
		return
	}

	if s.Metadata.Index < u.lastIndex() {
		u.ent = u.entriesFrom(s.Metadata.Index + 1)
		u.offset = s.Metadata.Index + 1
		u.shrinkEntries()
		u.snap = &s
	} else {
		u.restoreSnapshot(s)
	}
}

func (u *unstable) shrinkEntries() {
	if len(u.ent)*2 < cap(u.ent) {
		u.ent = slices.Clone(u.ent)
	}
}

func (u *unstable) restoreEntries(entries []Entry) { u.ent = entries }

func (u *unstable) restoreSnapshot(s Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.ent = []Entry{}
	u.snap = &s
}

func (u *unstable) append(entries ...Entry) {
	if len(entries) == 0 {
		return
	}

	after := entries[0].Index
	switch {
	case after == u.offset+int(len(u.ent)):
		u.ent = append(u.ent, entries...)
	case after <= u.offset:
		u.offset = after
		u.ent = entries
	default:
		u.ent = append([]Entry{}, u.slice(u.offset, after)...)
		u.ent = append(u.ent, entries...)
	}
}

func (u *unstable) slice(lo int, hi int) []Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.ent[lo-u.offset : hi-u.offset]
}

func (u *unstable) entriesFrom(i int) []Entry {
	return u.slice(i, u.lastIndex()+1)
}

func (u *unstable) entries() []Entry { return u.entriesFrom(u.firstIndex()) }

func (u *unstable) mustCheckOutOfBounds(lo, hi int) {
	if lo > hi {
		log.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + int(len(u.ent))
	if lo < u.offset || hi > upper {
		log.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
