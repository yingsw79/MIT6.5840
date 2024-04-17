package raft

import (
	"slices"
	"sync"
)

type raftLog struct {
	unstable
	commitIndex int
	lastApplied int

	applyCh chan ApplyMsg
	cond    *sync.Cond

	done chan struct{}
}

func newLog(applyCh chan ApplyMsg, cond *sync.Cond) *raftLog {
	return &raftLog{
		unstable: unstable{ent: []Entry{{}}},
		applyCh:  applyCh,
		cond:     cond,
		done:     make(chan struct{}),
	}
}

func (l *raftLog) kill() {
	close(l.done)
	l.cond.Signal()
}

func (l *raftLog) readyToApply() bool { return l.lastApplied < l.commitIndex }

func (l *raftLog) restoreSnapshot(s Snapshot) {
	l.unstable.restoreSnapshot(s)
	l.commitTo(s.Metadata.Index)
}

func (l *raftLog) commitTo(i int) bool {
	if i > l.commitIndex && i <= l.lastIndex() {
		l.commitIndex = i
		l.cond.Signal()
		return true
	}

	return false
}

func (l *raftLog) appliedTo(i int) { l.lastApplied = i }

func (l *raftLog) nextEntries() []Entry {
	i := max(l.lastApplied+1, l.firstIndex())
	if l.commitIndex+1 > i {
		return l.slice(i, l.commitIndex+1)
	}

	return nil
}

func (l *raftLog) nextSnapshot() *Snapshot {
	if s := l.snapshot(); s != nil && l.lastApplied < s.Metadata.Index {
		return s
	}

	return nil
}

func (l *raftLog) isUpToDate(term int, index int) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && index >= l.lastIndex())
}

func (l *raftLog) match(term int, index int) bool {
	if index < l.firstIndex()-1 || index > l.lastIndex() {
		return false
	}

	return l.term(index) == term
}

func (l *raftLog) maybeRestoreSnapshot(s Snapshot) bool {
	if s.Metadata.Index <= l.commitIndex {
		return false
	}

	if l.match(s.Metadata.Index, s.Metadata.Term) {
		l.commitTo(s.Metadata.Index)
		return false
	}

	l.restoreSnapshot(s)
	return true
}

func (l *raftLog) findConflictBackup(index int) FastBackup {
	backup := FastBackup{None, None, None}

	if index < l.firstIndex() {
		return backup
	} else if index > l.lastIndex() {
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
		l.truncateAndAppend(entries[i-index-1:]...)
	}

	l.commitTo(min(commit, lastMatchIndex))
	return lastMatchIndex, true
}

func (l *raftLog) applier() {
	for {
		select {
		case <-l.done:
			return
		default:
			l.cond.L.Lock()
			for !l.readyToApply() {
				l.cond.Wait()
			}

			s := l.nextSnapshot()
			ne := slices.Clone(l.nextEntries())
			l.cond.L.Unlock()

			if s != nil {
				select {
				case l.applyCh <- ApplyMsg{
					SnapshotValid: true,
					Snapshot:      s.Data,
					SnapshotTerm:  s.Metadata.Term,
					SnapshotIndex: s.Metadata.Index,
				}:
				case <-l.done:
					return
				}
				l.appliedTo(s.Metadata.Index)
			}

			for _, e := range ne {
				if e.Index == 0 {
					continue
				}

				select {
				case l.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      e.Command,
					CommandTerm:  e.Term,
					CommandIndex: e.Index,
				}:
				case <-l.done:
					return
				}
				l.appliedTo(e.Index)
			}
		}
	}
}
