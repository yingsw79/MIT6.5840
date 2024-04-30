package shardkv

import (
	"sync"

	"6.5840/kvraft"
	"6.5840/shardctrler"
)

type ShardStatus int

const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	GCing
)

type Shard struct {
	KV     kvraft.KVStateMachine
	Status ShardStatus
}

type MemoryShardKVStateMachine struct {
	mu                    sync.Mutex
	Shards                map[int]Shard
	CurConfig, LastConfig shardctrler.Config
	GID                   int
}

func (m *MemoryShardKVStateMachine) canServe(shardId int) bool {
	return m.CurConfig.Shards[shardId] == m.GID &&
		(m.Shards[shardId].Status == Serving || m.Shards[shardId].Status == GCing)
}

func (m *MemoryShardKVStateMachine) canPerformNextConfig() bool {
	for _, shard := range m.Shards {
		if shard.Status != Serving {
			return false
		}
	}
	return true
}

func (m *MemoryShardKVStateMachine) updateConfig() {
	
}

func (m *MemoryShardKVStateMachine) Check(args *kvraft.Args, reply *kvraft.Reply) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	shardId := key2shard(args.Op.(kvraft.Op).Key)
	if !m.canServe(shardId) {
		reply.Err = ErrWrongGroup
		return false
	}

	if !m.Shards[shardId].KV.Check(args, reply) {
		return false
	}

	return true
}
