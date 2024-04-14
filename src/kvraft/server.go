package kvraft

import (
	"bytes"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const timeout = time.Second

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

type Snapshot struct {
	StateMachine KVStateMachine
	LastOps      LastOps
}

type KVServer struct {
	// mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine KVStateMachine
	cache        *cache
	notifier     *notifier

	lastApplied int

	shutdown chan struct{}
}

func (srv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if v, ok := srv.cache.load(args.ClientId); ok {
		if v.Seq == args.Seq {
			reply.Value, reply.Err = v.Value, v.Err
			return
		} else if v.Seq > args.Seq {
			reply.Err = ErrDuplicateRequest
			return
		}
	}

	index, _, isLeader := srv.rf.Start(Op{Seq: args.Seq, Type: OpGet, Key: args.Key})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	replyCh := srv.notifier.register(index)

	select {
	case r := <-replyCh:
		reply.Value, reply.Err = r.Value, r.Err
	case <-time.After(timeout):
		reply.Err = ErrTimeout
	case <-srv.shutdown:
		reply.Err = ErrServerShutdown
	}

	go srv.notifier.unregister(index)
}

func (srv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if v, ok := srv.cache.load(args.ClientId); ok {
		if v.Seq == args.Seq {
			reply.Err = v.Err
			return
		} else if v.Seq > args.Seq {
			reply.Err = ErrDuplicateRequest
			return
		}
	}

	index, _, isLeader := srv.rf.Start(Op{Seq: args.Seq, Type: args.Type, Key: args.Key, Value: args.Value})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	replyCh := srv.notifier.register(index)

	select {
	case r := <-replyCh:
		reply.Err = r.Err
	case <-time.After(timeout):
		reply.Err = ErrTimeout
	case <-srv.shutdown:
		reply.Err = ErrServerShutdown
	}

	go srv.notifier.unregister(index)
}

func (srv *KVServer) applier() {
	for {
		select {
		case msg := <-srv.applyCh:
			if msg.CommandValid {
				if msg.CommandIndex <= srv.lastApplied {
					continue
				}

				var reply Reply
				op := msg.Command.(Op)
				if v, ok := srv.cache.load(op.ClientId); ok && v.Seq >= op.Seq {
					if v.Seq == op.Seq {
						reply.Value, reply.Err = v.Value, v.Err
					} else if v.Seq > op.Seq {
						reply.Err = ErrDuplicateRequest
					}
				} else {

				}

				var m OpContext
				switch op.Type {
				case OpGet:
				case OpPut:
				case OpAppend:
				}

			}
		case <-srv.shutdown:
			return
		}
	}
}

func (srv *KVServer) snapshot() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	snapshot := Snapshot{StateMachine: srv.stateMachine, LastOps: srv.cache.getLastOps()}
	if err := enc.Encode(&snapshot); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
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
	labgob.Register(MemoryKV{})

	srv := new(KVServer)
	srv.me = me
	srv.maxraftstate = maxraftstate

	// You may need initialization code here.
	srv.stateMachine = NewMemoryKV()

	srv.applyCh = make(chan raft.ApplyMsg)
	srv.rf = raft.Make(servers, me, persister, srv.applyCh)

	// You may need initialization code here.

	return srv
}
