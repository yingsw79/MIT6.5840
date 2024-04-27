package kvraft

import (
	"bytes"

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

type KVStore map[string]string

type Snapshot struct {
	KV      KVStore
	LastOps LastOps
}

type KVStateMachine interface {
	StateMachine
	Get(string) (string, Err)
	Put(string, string) Err
	Append(string, string) Err
}

type MemoryKVStateMachine struct {
	KV    KVStore
	Cache *Cache
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	labgob.Register(Op{})
	return &MemoryKVStateMachine{KV: make(map[string]string), Cache: NewCache()}
}

func (m *MemoryKVStateMachine) Get(k string) (string, Err) {
	if v, ok := m.KV[k]; ok {
		return v, OK
	}
	return "", ErrNoKey
}

func (m *MemoryKVStateMachine) Put(k, v string) Err {
	m.KV[k] = v
	return OK
}

func (m *MemoryKVStateMachine) Append(k, v string) Err {
	m.KV[k] += v
	return OK
}

func (m *MemoryKVStateMachine) IsDuplicate(clientId int64, seq int, reply *Reply) bool {
	return m.Cache.IsDuplicate(clientId, seq, reply)
}

func (m *MemoryKVStateMachine) ApplyCommand(msg any, reply *Reply) bool {
	c, ok := msg.(Command)
	if !ok {
		return false
	}

	if m.IsDuplicate(c.ClientId, c.Seq, reply) {
		return true
	}

	switch op := c.Op.(Op); op.Type {
	case OpGet:
		reply.Value, reply.Err = m.Get(op.Key)
	case OpPut:
		reply.Err = m.Put(op.Key, op.Value)
	case OpAppend:
		reply.Err = m.Append(op.Key, op.Value)
	default:
		return false
	}
	m.Cache.Store(c.ClientId, OpContext{Seq: c.Seq, Reply: *reply})
	return true
}

func (m *MemoryKVStateMachine) Snapshot() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	snapshot := Snapshot{KV: m.KV, LastOps: m.Cache.LastOps()}
	if err := enc.Encode(&snapshot); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *MemoryKVStateMachine) ApplySnapshot(data []byte) error {
	var snapshot Snapshot
	dec := labgob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&snapshot); err != nil {
		return err
	}

	m.KV = snapshot.KV
	m.Cache.SetLastOps(snapshot.LastOps)
	return nil
}
