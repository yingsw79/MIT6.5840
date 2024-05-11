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
	Check(*Args, *Reply) bool
}

type Args struct {
	ClientId int64
	Seq      int

	Op any
}

type Reply struct {
	Err   Err
	Value any
}
