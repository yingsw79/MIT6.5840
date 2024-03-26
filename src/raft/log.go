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

func (l *raftLog) lastLogTerm() uint32 {
	return l.term(l.lastLogIndex())
}

func (l *raftLog) term(i int) uint32 {
	return l.log[i].Term
}

// func (l *raftLog) lslice(i int) []Entry {
// 	return l.log[:i]
// }

func (l *raftLog) rslice(i int) []Entry {
	return l.log[i:]
}

func (l *raftLog) commitTo(i int) {
	if i > l.commitIndex && i <= l.lastLogIndex() {
		l.commitIndex = i
	}
}

func (l *raftLog) isUpToDate(term uint32, index int) bool {
	return term > l.lastLogTerm() || (term == l.lastLogTerm() && index >= l.lastLogIndex())
}

func (l *raftLog) append(st int, entries ...Entry) {
	if st >= 0 && st <= len(l.log) {
		l.log = append(l.log[:st], entries...)
	}
}

func (l *raftLog) findConflict(term uint32, index int) *FastBackup {
	backup := &FastBackup{}
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

func (l *raftLog) maybeAppend(term uint32, index int, committed int, entries []Entry) *FastBackup {
	backup := l.findConflict(term, index)
	if backup != nil {
		return backup
	}

	l.append(index+1, entries...) // TODO
	l.commitTo(min(committed, l.lastLogIndex()))

	return nil
}
