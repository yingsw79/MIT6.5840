package shardctrler

import (
	"6.5840/kvraft"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	*kvraft.Server
}

// func (sc *ShardCtrler) Join(args kvraft.IOp, reply *kvraft.Reply) {}

// func (sc *ShardCtrler) Leave(args kvraft.IOp, reply *kvraft.Reply) {}

// func (sc *ShardCtrler) Move(args kvraft.IOp, reply *kvraft.Reply) {}

// func (sc *ShardCtrler) Query(args kvraft.IOp, reply *kvraft.Reply) {}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft { return sc.Rf }

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})
	labgob.Register(Config{})
	labgob.Register(MemoryConfigStateMachine{})

	return &ShardCtrler{Server: kvraft.NewServer(servers, me, persister, -1, NewMemoryConfigStateMachine())}
}
