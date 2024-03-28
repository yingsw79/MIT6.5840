package raft

type raftLog struct {
	log         []Entry
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
}

func newLog(applyCh chan ApplyMsg) *raftLog {
	return &raftLog{log: []Entry{{}}, applyCh: applyCh}
}

func (l *raftLog) lastLogIndex() int          { return len(l.log) - 1 }
func (l *raftLog) len() int                   { return len(l.log) }
func (l *raftLog) lastLogTerm() int           { return l.log[len(l.log)-1].Term }
func (l *raftLog) term(i int) int             { return l.log[i].Term }
func (l *raftLog) rslice(i int) []Entry       { return l.log[i:] }
func (l *raftLog) entries() []Entry           { return l.log }
func (l *raftLog) setEntries(entries []Entry) { l.log = entries }

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

func (l *raftLog) append(entries ...Entry) { l.log = append(l.log, entries...) }

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

	t := l.log[index].Term
	backup.XTerm = t
	for ; index >= 0 && l.log[index].Term == t; index-- {
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
		l.log = append(l.log[:i], entries[i-index-1:]...)
	}
	if l.commitTo(min(commit, lastMatchIndex)) {
		l.apply()
	}

	return lastMatchIndex, true
}
