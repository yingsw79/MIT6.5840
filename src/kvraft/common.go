package kvraft

import "time"

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrServerTimeout  = "ErrServerTimeout"
	ErrServerShutdown = "ErrServerShutdown"
)

const timeout = 500 * time.Millisecond

type Err string

type StateMachine interface {
	ApplyCommand(any, *Reply) bool
	ApplySnapshot([]byte) error
	Snapshot() ([]byte, error)
	IsDuplicate(int64, int, *Reply) bool
}

type Command struct {
	ClientId int64
	Seq      int
	Op       any
}

type Reply struct {
	Err   Err
	Value any
}

type OpContext struct {
	Seq int
	Reply
}

type LastOps map[int64]OpContext
