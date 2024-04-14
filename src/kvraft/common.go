package kvraft

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrDuplicateRequest = "ErrDuplicateRequest"
	ErrTimeout          = "ErrTimeout"
	ErrServerShutdown   = "ErrServerShutdown"
)

type Err string

type OpType int

const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Type  OpType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      int
}

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
