package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"slices"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandTerm  int
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	StateFollower int = iota
	StateCandidate
	StateLeader
)

type HardState struct {
	CurrentTerm      int
	VotedFor         int
	SnapshotMetadata SnapshotMetadata
	Log              []Entry
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      bool                // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int
	votes       int
	currentTerm int
	votedFor    int
	leaderId    int

	tickInterval time.Duration

	heartbeatElapsed          int
	electionElapsed           int
	heartbeatTimeout          int
	electionTimeout           int
	randomizedElectionTimeout int

	tickf func()
	stepf func(any)

	log *raftLog

	msgHandlerCh chan msgHandler

	nextIndex  []int
	matchIndex []int

	shutdown chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (r *Raft) GetState() (int, bool) {
	// Your code here (2A).
	r.mu.Lock()
	defer r.mu.Unlock()

	term := r.currentTerm
	isLeader := r.state == StateLeader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (r *Raft) persist() {
	// Your code here (2C).
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)

	var snapshotMetadata SnapshotMetadata
	var snapshot []byte

	if s := r.log.snapshot(); s != nil {
		snapshotMetadata = s.Metadata
		snapshot = s.Data
	}

	st := HardState{
		CurrentTerm:      r.currentTerm,
		VotedFor:         r.votedFor,
		SnapshotMetadata: snapshotMetadata,
		Log:              r.log.entries(),
	}
	if err := enc.Encode(&st); err != nil {
		panic(err)
	}
	raftstate := buf.Bytes()

	r.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (r *Raft) readPersist() {
	// Your code here (2C).
	r.mu.Lock()
	defer r.mu.Unlock()

	if data := r.persister.ReadRaftState(); len(data) > 0 {
		var st HardState
		dec := labgob.NewDecoder(bytes.NewReader(data))
		if err := dec.Decode(&st); err != nil {
			panic(err)
		}
		r.currentTerm = st.CurrentTerm
		r.votedFor = st.VotedFor

		if snapshot := r.persister.ReadSnapshot(); len(snapshot) > 0 {
			r.log.restoreSnapshot(Snapshot{Metadata: st.SnapshotMetadata, Data: snapshot})
			r.log.restoreEntries(st.Log)
		} else {
			r.log.restoreEntries(st.Log)
		}
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (r *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	r.mu.Lock()
	defer r.mu.Unlock()

	if index < r.log.firstIndex() {
		return
	}

	r.log.stableTo(Snapshot{
		Metadata: SnapshotMetadata{Index: index, Term: r.log.term(index)},
		Data:     snapshot,
	})
	r.persist()
}

func (r *Raft) RaftStateSize() int { return r.persister.RaftStateSize() }

func (r *Raft) handleRequestVote(_args any, _reply any) {
	args := _args.(*RequestVoteArgs)
	reply := _reply.(*RequestVoteReply)

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term, None)
	}

	reply.Term = r.currentTerm
	if r.votedFor != None && r.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	if r.log.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		r.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
}

// example RequestVote RPC handler.
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	r.rpc(args, reply, r.handleRequestVote)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (r *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return r.peers[server].Call("Raft.RequestVote", args, reply)
}

func (r *Raft) broadcastRequestVote() {
	args := &RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  r.me,
		LastLogTerm:  r.log.lastTerm(),
		LastLogIndex: r.log.lastIndex(),
	}

	for i := range r.peers {
		if i == r.me {
			continue
		}

		go func() {
			reply := &RequestVoteReply{}
			if r.sendRequestVote(i, args, reply) {
				select {
				case r.msgHandlerCh <- rpcReplyMsgHandler{reply: reply, handler: r}:
				case <-r.shutdown:
				}
			}
		}()
	}
}

func (r *Raft) handleAppendEntries(_args any, _reply any) {
	args := _args.(*AppendEntriesArgs)
	reply := _reply.(*AppendEntriesReply)

	reply.FollowerId = r.me
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		return
	}

	r.becomeFollower(args.Term, args.LeaderId)
	reply.Term = r.currentTerm

	lastMatchIndex, ok := r.log.maybeAppend(args.PrevLogTerm, args.PrevLogIndex, args.LeaderCommit, args.Entries)
	if !ok {
		reply.Success = false
		reply.Backup = r.log.findConflictBackup(args.PrevLogIndex)
		return
	}

	reply.Success = true
	reply.LastMatchIndex = lastMatchIndex
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r.rpc(args, reply, r.handleAppendEntries)
}

func (r *Raft) sendAppendEntries(to int) {
	if r.maybeSendInstallSnapshot(to) {
		return
	}

	args := &AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderId:     r.leaderId,
		LeaderCommit: r.log.commitIndex,
		PrevLogIndex: r.nextIndex[to] - 1,
		PrevLogTerm:  r.log.term(r.nextIndex[to] - 1),
		Entries:      slices.Clone(r.log.entriesFrom(r.nextIndex[to])),
	}
	reply := &AppendEntriesReply{}

	go func() {
		if r.peers[to].Call("Raft.AppendEntries", args, reply) {
			select {
			case r.msgHandlerCh <- rpcReplyMsgHandler{reply: reply, handler: r}:
			case <-r.shutdown:
			}
		}
	}()
}

func (r *Raft) broadcastAppendEntries() {
	for i := range r.peers {
		if i != r.me {
			r.sendAppendEntries(i)
		}
	}
}

func (r *Raft) handleInstallSnapshot(_args any, _reply any) {
	args := _args.(*InstallSnapshotArgs)
	reply := _reply.(*InstallSnapshotReply)

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		return
	}

	r.becomeFollower(args.Term, args.LeaderId)

	if r.log.maybeRestoreSnapshot(args.Snapshot) {
		reply.LastMatchIndex = r.log.lastIndex()
	} else {
		reply.LastMatchIndex = r.log.commitIndex
	}

	reply.Term = r.currentTerm
	reply.FollowerId = r.me
	reply.Success = true
}

func (r *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	r.rpc(args, reply, r.handleInstallSnapshot)
}

func (r *Raft) maybeSendInstallSnapshot(to int) bool {
	s := r.log.snapshot()
	if s == nil || s.Metadata.Index < r.nextIndex[to] {
		return false
	}

	args := &InstallSnapshotArgs{
		Term:     r.currentTerm,
		LeaderId: r.leaderId,
		Snapshot: *s,
	}
	reply := &InstallSnapshotReply{}

	go func() {
		if r.peers[to].Call("Raft.InstallSnapshot", args, reply) {
			select {
			case r.msgHandlerCh <- rpcReplyMsgHandler{reply: reply, handler: r}:
			case <-r.shutdown:
			}
		}
	}()

	return true
}

func (r *Raft) rpc(args, reply any, handler func(any, any)) {
	msg := rpcMsgHandler{
		args:    args,
		reply:   reply,
		handler: handler,
		done:    make(chan struct{}),
	}

	select {
	case r.msgHandlerCh <- msg:
	case <-r.shutdown:
		return
	}
	select {
	case <-msg.done:
	case <-r.shutdown:
	}
}

func (r *Raft) becomeFollower(term int, LeaderId int) {
	r.reset(term)
	r.state = StateFollower
	r.leaderId = LeaderId
	r.tickf = r.tickElection
	r.stepf = r.stepFollower
}

func (r *Raft) becomeCandidate() {
	r.reset(r.currentTerm + 1)
	r.state = StateCandidate
	r.votedFor = r.me
	r.votes++
	r.tickf = r.tickElection
	r.stepf = r.stepCandidate
}

func (r *Raft) becomeLeader() {
	r.reset(r.currentTerm)
	r.state = StateLeader
	r.leaderId = r.me
	r.tickf = r.tickHeartbeat
	r.stepf = r.stepLeader

	for i := range r.peers {
		r.matchIndex[i] = 0
		r.nextIndex[i] = r.log.lastIndex() + 1
	}
	r.matchIndex[r.me] = r.log.lastIndex()
}

func (r *Raft) reset(term int) {
	if r.currentTerm != term {
		r.currentTerm = term
		r.votedFor = None
	}

	r.leaderId = None
	r.votes = 0
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *Raft) stepFollower(msg any) {
	switch msg := msg.(type) {
	case *AppendEntriesReply:
		if msg.Term > r.currentTerm {
			r.becomeFollower(msg.Term, None)
		}
	case *RequestVoteReply:
		if msg.Term > r.currentTerm {
			r.becomeFollower(msg.Term, None)
		}
	case *InstallSnapshotReply:
		if msg.Term > r.currentTerm {
			r.becomeFollower(msg.Term, None)
		}
	}
}

func (r *Raft) stepCandidate(msg any) {
	switch msg := msg.(type) {
	case *AppendEntriesReply:
		if msg.Term > r.currentTerm {
			r.becomeFollower(msg.Term, None)
		}
	case *RequestVoteReply:
		if msg.Term < r.currentTerm {
			return
		} else if msg.Term > r.currentTerm {
			r.becomeFollower(msg.Term, None)
		} else if msg.VoteGranted {
			r.votes++
			if r.votes >= len(r.peers)/2+1 {
				r.becomeLeader()
				r.broadcastAppendEntries()
			}
		}
	case *InstallSnapshotReply:
		if msg.Term > r.currentTerm {
			r.becomeFollower(msg.Term, None)
		}
	}
}

func (r *Raft) stepLeader(msg any) {
	switch msg := msg.(type) {
	case *AppendEntriesReply:
		if msg.Term < r.currentTerm {
			return
		} else if msg.Term > r.currentTerm {
			r.becomeFollower(msg.Term, None)
		} else if msg.Success {
			r.updateTrack(msg.FollowerId, msg.LastMatchIndex)
		} else {
			r.updateTrackWithBackup(msg.FollowerId, msg.Backup)
		}
	case *RequestVoteReply:
		if msg.Term > r.currentTerm {
			r.becomeFollower(msg.Term, None)
		}
	case *InstallSnapshotReply:
		if msg.Term < r.currentTerm {
			return
		} else if msg.Term > r.currentTerm {
			r.becomeFollower(msg.Term, None)
		} else if msg.Success {
			r.updateTrack(msg.FollowerId, msg.LastMatchIndex)
		}
	}
}

func (r *Raft) updateTrack(i, matched int) {
	r.matchIndex[i] = max(r.matchIndex[i], matched)
	r.nextIndex[i] = r.matchIndex[i] + 1

	if r.maybeCommit() {
		r.broadcastAppendEntries()
	}
}

func (r *Raft) updateTrackWithBackup(i int, backup FastBackup) {
	if backup.XTerm != None {
		r.nextIndex[i] = backup.XIndex
		if r.log.match(backup.XTerm, backup.XIndex) {
			r.nextIndex[i]++
		}
		r.sendAppendEntries(i)
	} else if backup.XLen != None {
		r.nextIndex[i] = backup.XLen
		r.sendAppendEntries(i)
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		r.becomeCandidate()
		r.broadcastRequestVote()
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.broadcastAppendEntries()
	}
}

func (r *Raft) quorumIndex() int {
	m := slices.Clone(r.matchIndex)
	slices.Sort(m)
	return m[len(r.peers)/2]
}

func (r *Raft) maybeCommit() bool {
	if i := r.quorumIndex(); r.log.term(i) == r.currentTerm {
		return r.log.commitTo(i)
	}
	return false
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (r *Raft) Start(command any) (int, int, bool) {
	// Your code here (2B).
	r.mu.Lock()
	defer r.mu.Unlock()

	index := None
	term := r.currentTerm
	isLeader := r.state == StateLeader
	if !isLeader {
		return index, term, isLeader
	}

	index = r.log.lastIndex() + 1
	r.log.append(Entry{Term: term, Index: index, Command: command})
	r.matchIndex[r.me]++
	r.broadcastAppendEntries()
	r.persist()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (r *Raft) Kill() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.killed() {
		return
	}
	r.kill()
}

func (r *Raft) kill() {
	close(r.shutdown)
	r.dead = true
	r.log.kill()
}

func (r *Raft) killed() bool { return r.dead }

func (r *Raft) tick() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.killed() {
		return
	}
	r.tickf()
	r.persist()
}

func (r *Raft) handle(h msgHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.killed() {
		return
	}
	h.handle()
	r.persist()
}

func (r *Raft) ticker() {
	tk := time.NewTicker(r.tickInterval)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			r.tick()
		case h := <-r.msgHandlerCh:
			r.handle(h)
		case <-r.shutdown:
			return
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	r := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		tickInterval:     50 * time.Millisecond,
		heartbeatTimeout: 1,
		electionTimeout:  10,
		votedFor:         None,
		nextIndex:        make([]int, len(peers)),
		matchIndex:       make([]int, len(peers)),
		msgHandlerCh:     make(chan msgHandler),
		shutdown:         make(chan struct{}),
	}
	r.log = newLog(applyCh, sync.NewCond(&r.mu))
	// Your initialization code here (2A, 2B, 2C).
	r.becomeFollower(0, None)
	// initialize from state persisted before a crash
	r.readPersist()

	// start ticker goroutine to start elections
	go r.ticker()
	go r.log.applier()

	return r
}
