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

type AppendEntriesArgs struct {
	Term         uint32
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  uint32
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    uint32
	Success bool
}
