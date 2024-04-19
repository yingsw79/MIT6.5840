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

type OpType int

const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

type IOp interface {
	GetClientId() int64
	GetSeq() int
}

type StateMachine interface{ Apply(IOp) Reply }

type Op struct {
	ClientId int64
	Seq      int

	Type  OpType
	Key   string
	Value string
}

func (op Op) GetClientId() int64 { return op.ClientId }
func (op Op) GetSeq() int        { return op.Seq }

type Reply struct {
	Err   Err
	Value any
}
