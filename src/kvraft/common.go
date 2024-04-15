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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      int
	Type     OpType
	Key      string
	Value    string
}

// Put or Append
type PutAppendArgs Op

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	Seq      int
}

type GetReply Reply

type Reply struct {
	Err   Err
	Value string
}
