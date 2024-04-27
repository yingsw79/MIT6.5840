package shardctrler

import (
	"6.5840/kvraft"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct{ *kvraft.Server }

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft { return sc.Rf }

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	return &ShardCtrler{Server: kvraft.NewServer(servers, me, persister, -1, NewMemoryConfigStateMachine())}
}
