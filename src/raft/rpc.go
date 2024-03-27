package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type Entry struct {
	Term    int
	Index   int
	Command any
}

type FastBackup struct {
	XTerm  int
	XIndex int
	XLen   int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term           int
	Success        bool
	FollowerId     int
	LastMatchIndex int
	Backup         FastBackup
}

// type HeartbeatArgs struct {
// 	Term         int
// 	LeaderId     int
// 	LeaderCommit int
// }

// type HeartbeatReply struct {
// 	Term    int
// 	Success bool
// }
