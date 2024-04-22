package shardkv

import "6.5840/kvraft"

type ShardKVStateMachine interface {
	kvraft.KVStateMachine
}

type MemoryShardKVStateMachine struct {
	kvraft.MemoryKVStateMachine
	
}
