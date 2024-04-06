package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrShutdown    = "ErrShutdown"
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
	Id int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int
}

type GetReply struct {
	Err   Err
	Value string
}

type rpcMsgHandler struct {
	id    int
	args  any
	reply any
	done  chan struct{}
}

type appliedMsg struct {
	id    int
	err   Err
	value string
}

func (h *rpcMsgHandler) handle(m appliedMsg) {
	switch reply := h.reply.(type) {
	case *GetReply:
		reply.Value = m.value
		reply.Err = m.err
		close(h.done)
	case *PutAppendReply:
		reply.Err = m.err
		close(h.done)
	}
}
