package raft

type raftLog struct {
	log         []Entry
	commitIndex int
}

func newLog() *raftLog {
	return &raftLog{log: []Entry{{}}}
}

func (l *raftLog) lastLogIndex() int {
	return len(l.log) - 1
}

func (l *raftLog) lastLogTerm() int {
	return l.log[len(l.log)-1].Term
}

func (l *raftLog) term(i int) int {
	return l.log[i].Term
}

func (l *raftLog) rslice(i int) []Entry {
	return l.log[i:]
}

func (l *raftLog) commitTo(i int) bool {
	if i > l.commitIndex && i <= l.lastLogIndex() {
		l.commitIndex = i
		return true
	}
	return false
}

func (l *raftLog) isUpToDate(term int, index int) bool {
	return term > l.lastLogTerm() || (term == l.lastLogTerm() && index >= l.lastLogIndex())
}

func (l *raftLog) append(entries ...Entry) {
	l.log = append(l.log, entries...)
}

func (l *raftLog) findConflict(term int, index int) *FastBackup {
	backup := &FastBackup{None, None, None}
	if index > l.lastLogIndex() {
		backup.XLen = index - l.lastLogIndex()
		return backup
	}

	t := l.log[index].Term
	if t == term {
		return nil
	}

	backup.XTerm = t
	for ; index >= 0 && l.log[index].Term == t; index-- {
	}
	backup.XIndex = index + 1

	return backup
}

func (l *raftLog) maybeAppend(term int, index int, committed int, entries []Entry) *FastBackup {
	backup := l.findConflict(term, index)
	if backup != nil {
		return backup
	}
	l.log = append(l.log[:index+1], entries...) // TODO: Figure 8
	l.commitTo(min(committed, l.lastLogIndex()))

	return nil
}
