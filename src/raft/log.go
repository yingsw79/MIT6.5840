package raft

import "slices"

type raftLog struct {
	unstable    unstable
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
}

func newLog(applyCh chan ApplyMsg) *raftLog {
	return &raftLog{unstable: unstable{entries: []Entry{{}}}, applyCh: applyCh}
}

func (l *raftLog) firstIndex() int {
	i, _ := l.unstable.maybeFirstIndex()
	return i
}

func (l *raftLog) lastLogIndex() int {
	i, _ := l.unstable.maybeLastIndex()
	return i
}

func (l *raftLog) len() int { return l.lastLogIndex() + 1 }

func (l *raftLog) lastLogTerm() int {
	t, _ := l.unstable.maybeTerm(l.lastLogIndex())
	return t
}

func (l *raftLog) term(i int) int {
	t, _ := l.unstable.maybeTerm(i)
	return t
}

func (l *raftLog) stableTo(i, t int) { l.unstable.stableTo(i, t) } // TODO

func (l *raftLog) stableSnapTo(i int) { l.unstable.stableSnapTo(i) }

func (l *raftLog) slice(lo, hi int) []Entry {
	return slices.Clone(l.unstable.slice(max(lo, l.unstable.offset), hi))
}

func (l *raftLog) entries(i int) []Entry { return l.slice(i, l.lastLogIndex()+1) }

func (l *raftLog) allEntries() []Entry { return l.entries(l.firstIndex()) }

// func (l *raftLog) setEntries(entries []Entry) { l.log = slices.Clone(entries) }

func (l *raftLog) commitTo(i int) bool {
	if i > l.commitIndex && i <= l.lastLogIndex() {
		l.commitIndex = i
		return true
	}

	return false
}

func (l *raftLog) apply() {
	for ; l.lastApplied <= l.commitIndex; l.lastApplied++ {
		e := l.log[l.lastApplied]
		if e.Command == nil {
			continue
		}

		l.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      e.Command,
			CommandIndex: e.Index,
		}
	}
}

func (l *raftLog) isUpToDate(term int, index int) bool {
	return term > l.lastLogTerm() || (term == l.lastLogTerm() && index >= l.lastLogIndex())
}

func (l *raftLog) append(entries ...Entry) { l.unstable.truncateAndAppend(entries) }

func (l *raftLog) match(term int, index int) bool {
	if index > l.lastLogIndex() {
		return false
	}

	return l.term(index) == term
}

func (l *raftLog) findConflictBackup(index int) FastBackup {
	backup := FastBackup{None, None, None}
	if index > l.lastLogIndex() {
		backup.XLen = l.len()
		return backup
	}

	t := l.term(index)
	backup.XTerm = t
	for ; index >= 0 && l.term(index) == t; index-- {
	}
	backup.XIndex = index + 1

	return backup
}

func (l *raftLog) findConflictIndex(entries []Entry) int {
	for _, e := range entries {
		if !l.match(e.Term, e.Index) {
			return e.Index
		}
	}

	return None
}

func (l *raftLog) maybeAppend(term int, index int, commit int, entries []Entry) (int, bool) {
	if !l.match(term, index) {
		return None, false
	}

	lastMatchIndex := index + len(entries)
	i := l.findConflictIndex(entries)
	if i != None {
		l.append(entries[i-index-1:]...)
	}
	if l.commitTo(min(commit, lastMatchIndex)) {
		l.apply()
	}

	return lastMatchIndex, true
}
