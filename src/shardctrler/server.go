package shardctrler

import (
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	stateMachine ConfigStateMachine
	lastApplied  int

	shutdown chan struct{}
}

type OpType int

const (
	OpJoin OpType = iota
	OpLeave
	OpMove
	OpQuery
)

type Op struct {
	// Your data here.
	ClientId   int64
	Seq        int
	Type       OpType
	Servers    map[int][]string // Join
	GIDs       []int            // Leave
	Shard, GID int              // Move
	Num        int              // Query
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})

	applyCh := make(chan raft.ApplyMsg)
	sc := &ShardCtrler{
		me:           me,
		applyCh:      applyCh,
		rf:           raft.Make(servers, me, persister, applyCh),
		stateMachine: NewMemoryConfigStateMachine(),
		shutdown:     make(chan struct{}),
	}

	// Your code here.

	return sc
}
