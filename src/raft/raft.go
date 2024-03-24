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
	"cmp"
	"log"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

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
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

const None int = -1

const (
	StateFollower uint32 = iota
	StateCandidate
	StateLeader
)

type Entry struct {
	Term  uint32
	Index int
	Data  []byte
}

type msgHandler interface {
	handle()
}

type rpcMsgHandler struct {
	args    any
	reply   any
	handler func(any, any)
	done    chan struct{}
}

func (h rpcMsgHandler) handle() {
	h.handler(h.args, h.reply)
	close(h.done)
}

type rpcReplyMsgHandler struct {
	reply   any
	handler *Raft
}

func (h rpcReplyMsgHandler) handle() {
	h.handler.step(h.reply)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state uint32
	votes int

	electionElapsed  int
	heartbeatElapsed int

	tickInterval              time.Duration
	heartbeatTimeout          int
	electionTimeout           int
	randomizedElectionTimeout int

	applyCh      chan ApplyMsg
	msgHandlerCh chan msgHandler
	done         chan struct{}

	tick func()
	step func(any)

	currentTerm uint32
	votedFor    int
	log         []Entry

	leaderId int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (r *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(atomic.LoadUint32(&r.currentTerm)), atomic.LoadUint32(&r.state) == StateLeader
	// r.mu.Lock()
	// defer r.mu.Unlock()
	// return int(r.currentTerm), r.state == StateLeader
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (r *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (r *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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

	var lastLogTerm uint32
	var lastLogIndex int
	if len(r.log) > 0 {
		lastLog := r.log[len(r.log)-1]
		lastLogTerm, lastLogIndex = lastLog.Term, lastLog.Index
	}

	if cmp.Or(cmp.Compare(args.LastLogTerm, lastLogTerm), cmp.Compare(args.LastLogIndex, lastLogIndex)) >= 0 {
		reply.VoteGranted = true
		r.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
}

// example RequestVote RPC handler.
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	msg := rpcMsgHandler{args: args, reply: reply, handler: r.handleRequestVote, done: make(chan struct{})}
	select {
	case r.msgHandlerCh <- msg:
	case <-r.done:
		return
	}
	select {
	case <-msg.done:
	case <-r.done:
	}
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

func (r *Raft) handleAppendEntries(_args any, _reply any) {
	args := _args.(*AppendEntriesArgs)
	reply := _reply.(*AppendEntriesReply)

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		return
	}

	r.becomeFollower(args.Term, args.LeaderId)

	reply.Term = r.currentTerm
	if len(args.Entries) == 0 {
		reply.Success = true
		return
	}

	i, ok := slices.BinarySearchFunc(r.log, Entry{Term: args.PrevLogTerm, Index: args.PrevLogIndex}, func(e1, e2 Entry) int {
		return cmp.Or(cmp.Compare(e1.Term, e2.Term), cmp.Compare(e1.Index, e2.Index))
	})
	if !ok {
		reply.Success = false
		return
	}

	r.log = append(r.log[i+1:], args.Entries...)

	if args.LeaderCommit > r.commitIndex {
		r.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Index)
	}

	reply.Success = true
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	msg := rpcMsgHandler{args: args, reply: reply, handler: r.handleAppendEntries, done: make(chan struct{})}
	select {
	case r.msgHandlerCh <- msg:
	case <-r.done:
		return
	}
	select {
	case <-msg.done:
	case <-r.done:
	}
}

func (r *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return r.peers[server].Call("Raft.AppendEntries", args, reply)
}

// func (r *Raft) broadcastAppendEntries() {
// 	successCount := 1

// 	wg := sync.WaitGroup{}
// 	for i := range r.peers {
// 		if i == r.me {
// 			continue
// 		}

// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()

// 			args := &AppendEntriesArgs{
// 				Term:         r.currentTerm,
// 				LeaderId:     r.leaderId,
// 				LeaderCommit: r.commitIndex,
// 			}
// 			idx := len(r.log) - 1
// 			for {
// 				var prevLogIndex, prevLogTerm int
// 				if idx > 0 {
// 					prevEntry := r.log[idx-1]
// 					prevLogIndex, prevLogTerm = prevEntry.Index, prevEntry.Term
// 				}
// 				args.PrevLogIndex = prevLogIndex
// 				args.PrevLogTerm = prevLogTerm
// 				args.Entries = r.log[idx:]
// 				reply := &AppendEntriesReply{}
// 				if r.sendAppendEntries(i, args, reply) {
// 					if reply.Success {
// 						return
// 					} else {
// 						idx--
// 					}
// 				} else {
// 					return
// 				}
// 			}
// 		}()
// 	}
// 	wg.Wait()

// 	if successCount > len(r.peers)/2+1 {
// 		r.commitIndex++
// 		r.lastApplied++
// 	}
// }

func (r *Raft) becomeFollower(term uint32, LeaderId int) {
	r.reset(term)
	r.state = StateFollower
	r.leaderId = LeaderId
	r.tick = r.tickElection
	r.step = r.stepFollower
	log.Printf("%d became follower at term %d", r.me, r.currentTerm)
}

func (r *Raft) becomeCandidate() {
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}

	r.reset(r.currentTerm + 1)
	r.state = StateCandidate
	r.votedFor = r.me
	r.votes++
	r.tick = r.tickElection
	r.step = r.stepCandidate
	log.Printf("%d became candidate at term %d", r.me, r.currentTerm)
}

func (r *Raft) becomeLeader() {
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}

	r.reset(r.currentTerm)
	r.state = StateLeader
	r.leaderId = r.me
	r.tick = r.tickHeartbeat
	r.step = r.stepLeader

	idx := 1
	if len(r.log) > 0 {
		idx = r.log[len(r.log)-1].Index + 1
	}
	for i := range r.peers {
		r.matchIndex[i] = 0
		r.nextIndex[i] = idx
	}
	log.Printf("%d became leader at term %d", r.me, r.currentTerm)
}

func (r *Raft) reset(term uint32) {
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

func (r *Raft) stepFollower(reply any) {
	switch reply := reply.(type) {
	case *AppendEntriesReply:
		if reply.Term > r.currentTerm {
			r.becomeFollower(reply.Term, None)
		}
	case *RequestVoteReply:
		if reply.Term > r.currentTerm {
			r.becomeFollower(reply.Term, None)
		}
	}
}

func (r *Raft) stepCandidate(reply any) {
	switch reply := reply.(type) {
	case *AppendEntriesReply:
		if reply.Term > r.currentTerm {
			r.becomeFollower(reply.Term, None)
		}
	case *RequestVoteReply:
		if reply.Term < r.currentTerm {
			return
		} else if reply.Term > r.currentTerm {
			r.becomeFollower(reply.Term, None)
		} else if reply.VoteGranted {
			r.votes++
			if r.votes >= len(r.peers)/2+1 {
				r.becomeLeader()
				r.broadcastHeartbeat()
			}
		}
	}
}

func (r *Raft) stepLeader(reply any) {
	switch reply := reply.(type) {
	case *AppendEntriesReply:
		if reply.Term > r.currentTerm {
			r.becomeFollower(reply.Term, None)
		}
		// TODO
	case *RequestVoteReply:
		if reply.Term > r.currentTerm {
			r.becomeFollower(reply.Term, None)
		}
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

func (r *Raft) broadcastRequestVote() {
	var lastLogTerm uint32
	var lastLogIndex int
	if len(r.log) > 0 {
		lastLog := r.log[len(r.log)-1]
		lastLogTerm, lastLogIndex = lastLog.Term, lastLog.Index
	}
	args := &RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  r.me,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
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
				case <-r.done:
				}
			}
		}()
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.broadcastHeartbeat()
	}
}

func (r *Raft) broadcastHeartbeat() {
	args := &AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderId:     r.leaderId,
		LeaderCommit: r.commitIndex,
	}

	for i := range r.peers {
		if i == r.me {
			continue
		}

		go func() {
			reply := &AppendEntriesReply{}
			if r.sendAppendEntries(i, args, reply) {
				select {
				case r.msgHandlerCh <- rpcReplyMsgHandler{reply: reply, handler: r}:
				case <-r.done:
				}
			}
		}()
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	atomic.StoreInt32(&r.dead, 1)
	// Your code here, if desired.
	close(r.done)
}

// func (r *Raft) killed() bool {
// 	z := atomic.LoadInt32(&r.dead)
// 	return z == 1
// }

func (r *Raft) ticker() {
	tk := time.NewTicker(r.tickInterval)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			r.tick()
		case h := <-r.msgHandlerCh:
			h.handle()
		case <-r.done:
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
		applyCh:          applyCh,
		tickInterval:     50 * time.Millisecond,
		heartbeatTimeout: 1,
		electionTimeout:  10,
		votedFor:         None,
		nextIndex:        make([]int, len(peers)),
		matchIndex:       make([]int, len(peers)),
		msgHandlerCh:     make(chan msgHandler),
		done:             make(chan struct{}),
	}
	// Your initialization code here (2A, 2B, 2C).
	r.becomeFollower(0, None)
	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go r.ticker()

	return r
}
