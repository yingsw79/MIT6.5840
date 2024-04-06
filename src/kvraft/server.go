package kvraft

import (
	"log"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// type opArgs struct {
// 	OpType opType
// 	Key    string
// 	Value  string
// }

// type opReply struct {
// 	Value string
// 	Err   error
// 	Done  chan struct{}
// }

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Id    int
	Key   string
	Value string
}

type KVServer struct {
	// mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvmap   map[string]string
	cache   map[int]appliedMsg
	pending map[int]rpcMsgHandler

	msgHandlerCh chan rpcMsgHandler
	shutdown     chan struct{}
}

func (srv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := srv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	done := make(chan struct{})
	msg := rpcMsgHandler{
		id:    args.Id,
		reply: reply,
		done:  done,
	}

	select {
	case srv.msgHandlerCh <- msg:
	case <-srv.shutdown:
		reply.Err = ErrShutdown
		return
	}

	// srv.rf.Start(Op{Type: OpGet, Id: args.Id, Key: args.Key})

	select {
	case <-done:
	case <-srv.shutdown:
		reply.Err = ErrShutdown
	}
}

func (srv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := srv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	done := make(chan struct{})
	msg := rpcMsgHandler{
		id:    args.Id,
		reply: reply,
		done:  done,
	}

	select {
	case srv.msgHandlerCh <- msg:
	case <-srv.shutdown:
		reply.Err = ErrShutdown
		return
	}

	// srv.rf.Start(Op{Type: args.Type, Id: args.Id, Key: args.Key, Value: args.Value})

	select {
	case <-done:
	case <-srv.shutdown:
		reply.Err = ErrShutdown
	}
}

func (srv *KVServer) applier() {
	for {
		select {
		case am := <-srv.applyCh:
			if am.CommandValid {
				op := am.Command.(Op)
				var m appliedMsg
				switch op.Type {
				case OpGet:
					if v, ok := srv.kvmap[op.Key]; ok {
						m.value = v
					} else {
						m.err = ErrNoKey
					}
				case OpPut:
					srv.kvmap[op.Key] = op.Value
				case OpAppend:
					srv.kvmap[op.Key] += op.Value
				}

				h := srv.pending[op.Id]
				h.handle(m)
				srv.cache[h.id] = m
				delete(srv.pending, op.Id)
			}
		case h := <-srv.msgHandlerCh:
			if m, ok := srv.cache[h.id]; ok {
				h.handle(m)
			} else {
				switch args := h.args.(type) {
				case *GetReply:
					args.Value = m.value
					args.Err = m.err
					close(h.done)
				case *PutAppendReply:
					args.Err = m.err
					close(h.done)
				}
				srv.pending[h.id] = h
				// srv.rf.Start(Op{Type: args.Type, Id: args.Id, Key: args.Key, Value: args.Value})
			}
		case <-srv.shutdown:
			return
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (srv *KVServer) Kill() {
	atomic.StoreInt32(&srv.dead, 1)
	srv.rf.Kill()
	// Your code here, if desired.
}

func (srv *KVServer) killed() bool {
	z := atomic.LoadInt32(&srv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	srv := new(KVServer)
	srv.me = me
	srv.maxraftstate = maxraftstate

	// You may need initialization code here.

	srv.applyCh = make(chan raft.ApplyMsg)
	srv.rf = raft.Make(servers, me, persister, srv.applyCh)

	// You may need initialization code here.

	return srv
}
