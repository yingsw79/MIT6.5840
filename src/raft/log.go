package raft

type raftLog struct {
	unstable
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
}

func newLog(applyCh chan ApplyMsg) *raftLog {
	return &raftLog{unstable: unstable{ent: []Entry{{}}}, applyCh: applyCh}
}

func (l *raftLog) commitTo(i int) bool {
	if i > l.commitIndex && i <= l.lastIndex() {
		l.commitIndex = i
		return true
	}

	return false
}

func (l *raftLog) appliedTo(i int) {
	if i > l.lastApplied && i <= l.commitIndex {
		l.lastApplied = i
	}
}

func (l *raftLog) apply() {
	if l.hasPendingSnapshot() {
		snapshot := l.snapshot()
		l.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot.Data,
			SnapshotTerm:  snapshot.Term,
			SnapshotIndex: snapshot.Index,
		}
		l.appliedTo(l.snapshotIndex())
	}

	ne := l.nextEntries()
	for _, e := range ne {
		if e.Command == nil {
			continue
		}

		l.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      e.Command,
			CommandIndex: e.Index,
		}
		l.appliedTo(e.Index)
	}
}

func (l *raftLog) nextEntries() []Entry {
	i := max(l.lastApplied+1, l.firstIndex())
	if l.commitIndex+1 > i {
		return l.slice(i, l.commitIndex+1)
	}
	return nil
}

func (l *raftLog) hasPendingSnapshot() bool {
	return l.hasSnapshot() && l.lastApplied < l.snapshotIndex()
}

func (l *raftLog) isUpToDate(term int, index int) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && index >= l.lastIndex())
}

func (l *raftLog) match(term int, index int) bool {
	if index > l.lastIndex() {
		return false
	}

	return l.term(index) == term
}

func (l *raftLog) findConflictBackup(index int) FastBackup {
	backup := FastBackup{None, None, None}
	if index > l.lastIndex() {
		backup.XLen = l.lastIndex() + 1
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
