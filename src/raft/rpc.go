package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         uint32
	CandidateId  int
	LastLogIndex int
	LastLogTerm  uint32
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        uint32
	VoteGranted bool
}

type Entry struct {
	Term    uint32
	Index   int
	Command any
}

type FastBackup struct {
	XTerm  uint32
	XIndex int
	XLen   int
}

type AppendEntriesArgs struct {
	Term         uint32
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  uint32
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term       uint32
	Success    bool
	FollowerId int
	Backup     FastBackup
}

// type HeartbeatArgs struct {
// 	Term         uint32
// 	LeaderId     int
// 	LeaderCommit int
// }

// type HeartbeatReply struct {
// 	Term    uint32
// 	Success bool
// }
