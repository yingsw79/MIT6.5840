package kvraft

import (
	"bytes"
	"sync"

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

type KVStateMachine interface {
	StateMachine
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

type KVStore map[string]string

func NewKVStore() KVStore { return KVStore{} }

func (kv KVStore) Get(k string) (string, Err) {
	if v, ok := kv[k]; ok {
		return v, OK
	}
	return "", ErrNoKey
}

func (kv KVStore) Put(k, v string) Err {
	kv[k] = v
	return OK
}

func (kv KVStore) Append(k, v string) Err {
	kv[k] += v
	return OK
}

type MemoryKVStateMachine struct {
	mu sync.Mutex
	KVStore
	Cache Cache
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	labgob.Register(Op{})
	return &MemoryKVStateMachine{KVStore: NewKVStore(), Cache: NewCache()}
}

func (m *MemoryKVStateMachine) Check(args *Args, reply *Reply) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.Cache.Check(args, reply)
}

func (m *MemoryKVStateMachine) ApplyCommand(msg any, reply *Reply) bool {
	args, ok := msg.(Args)
	if !ok {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.Cache.Check(&args, reply) {
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

	m.Cache.Store(args.ClientId, OpContext{Seq: args.Seq, Reply: *reply})
	return true
}

func (m *MemoryKVStateMachine) Snapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	if err := enc.Encode(m.KVStore); err != nil {
		return nil, err
	}
	if err := enc.Encode(m.Cache); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *MemoryKVStateMachine) ApplySnapshot(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	dec := labgob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&m.KVStore); err != nil {
		return err
	}
	if err := dec.Decode(&m.Cache); err != nil {
		return err
	}
	return nil
}
