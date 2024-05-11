package kvraft

import (
	"6.5840/labgob"
)

type OpType int

const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

type Op struct {
	Type  OpType
	Key   string
	Value string
}

type OpContext struct {
	Seq int
	Reply
}

type KV interface {
	Get(string) (string, Err)
	Put(string, string) Err
	Append(string, string) Err
}

type Cache map[int64]OpContext

func NewCache() Cache { return Cache{} }

func (c Cache) Store(k int64, v OpContext) { c[k] = v }

func (c Cache) Check(args *Args, reply *Reply) bool {
	if v, ok := c[args.ClientId]; ok && v.Seq >= args.Seq {
		reply.Value, reply.Err = v.Value, v.Err
		return false
	}
	return true
}

type MemoryKV map[string]string

func NewMemoryKV() MemoryKV { return MemoryKV{} }

func (kv MemoryKV) Get(k string) (string, Err) {
	if v, ok := kv[k]; ok {
		return v, OK
	}
	return "", ErrNoKey
}

func (kv MemoryKV) Put(k, v string) Err {
	kv[k] = v
	return OK
}

func (kv MemoryKV) Append(k, v string) Err {
	kv[k] += v
	return OK
}

type MemoryKVStateMachine struct {
	KV
	Cache
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	labgob.Register(Op{})
	labgob.Register(MemoryKV{})
	return &MemoryKVStateMachine{KV: NewMemoryKV(), Cache: NewCache()}
}

func (m *MemoryKVStateMachine) ApplyCommand(msg any, reply *Reply) bool {
	args, ok := msg.(Args)
	if !ok {
		return false
	}

	if !m.Check(&args, reply) {
		return false
	}

	switch op := args.Op.(Op); op.Type {
	case OpGet:
		reply.Value, reply.Err = m.Get(op.Key)
	case OpPut:
		reply.Err = m.Put(op.Key, op.Value)
	case OpAppend:
		reply.Err = m.Append(op.Key, op.Value)
	default:
		return false
	}

	m.Store(args.ClientId, OpContext{Seq: args.Seq, Reply: *reply})
	return true
}
