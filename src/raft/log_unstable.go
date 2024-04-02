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

func (u *unstable) firstIndex() int { return u.offset }

func (u *unstable) hasSnapshot() bool { return u.snap != nil }

func (u *unstable) snapshot() Snapshot { return *u.snap }

func (u *unstable) snapshotIndex() int { return u.snap.Metadata.Index }

func (u *unstable) lastIndex() int {
	if l := len(u.ent); l != 0 {
		return u.offset + int(l) - 1
	}
	if u.hasSnapshot() {
		return u.snapshotIndex()
	}
	return 0
}

func (u *unstable) lastTerm() int { return u.term(u.lastIndex()) }

func (u *unstable) term(i int) int {
	if i < u.offset {
		if u.hasSnapshot() && u.snapshotIndex() == i {
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

// func (u *unstable) stableTo(s Snapshot) { // TODO
// 	if s.Term == u.term(s.Index) && s.Index >= u.offset && s.Index <= u.lastIndex() {
// 		u.ent = u.entriesFrom(s.Index + 1)
// 		u.offset = s.Index + 1
// 		u.shrinkEntries()
// 		u.snap = &s
// 	} else if s.Index > u.lastIndex() {
// 		u.restoreSnapshot(s)
// 	}
// }

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
	after := entries[0].Index
	switch {
	case after == u.offset+int(len(u.ent)):
		// after is the next index in the u.entries
		// directly append
		u.ent = append(u.ent, entries...)
	case after <= u.offset: // TODO
		log.Printf("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		u.offset = after
		u.ent = entries
	default:
		// truncate to after and copy to u.entries
		// then append
		log.Printf("truncate the unstable entries before index %d", after)
		u.ent = append([]Entry{}, u.slice(u.offset, after)...)
		u.ent = append(u.ent, entries...)
	}
}

func (u *unstable) slice(lo int, hi int) []Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.ent[lo-u.offset : hi-u.offset]
}

func (u *unstable) entriesFrom(i int) []Entry {
	return u.slice(max(i, u.firstIndex()), u.lastIndex()+1)
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
