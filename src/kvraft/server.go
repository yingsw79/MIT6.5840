package kvraft

import (
	"bytes"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

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
	if v, ok := srv.cache.load(args.ClientId); ok && v.Seq >= args.Seq {
		reply.Value, reply.Err = v.Value, v.Err
		return
	}

	index, _, isLeader := srv.rf.Start(Op{ClientId: args.ClientId, Seq: args.Seq, Type: OpGet, Key: args.Key})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	replyCh := srv.notifier.register(index)

	select {
	case r := <-replyCh:
		reply.Value, reply.Err = r.Value, r.Err
	case <-time.After(timeout):
		reply.Err = ErrServerTimeout
	case <-srv.shutdown:
		reply.Err = ErrServerShutdown
	}

	go srv.notifier.unregister(index)
}

func (srv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if v, ok := srv.cache.load(args.ClientId); ok && v.Seq >= args.Seq {
		reply.Err = v.Err
		return
	}

	index, _, isLeader := srv.rf.Start(Op(*args))
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	replyCh := srv.notifier.register(index)

	select {
	case r := <-replyCh:
		reply.Err = r.Err
	case <-time.After(timeout):
		reply.Err = ErrServerTimeout
	case <-srv.shutdown:
		reply.Err = ErrServerShutdown
	}

	go srv.notifier.unregister(index)
}

func (srv *KVServer) applyCommand(op *Op) (reply Reply) {
	switch op.Type {
	case OpGet:
		reply.Value, reply.Err = srv.stateMachine.Get(op.Key)
	case OpPut:
		reply.Err = srv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		reply.Err = srv.stateMachine.Append(op.Key, op.Value)
	}
	return
}

func (srv *KVServer) applySnapshot(data []byte) error {
	var snapshot Snapshot
	dec := labgob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&snapshot); err != nil {
		return err
	}

	srv.stateMachine = snapshot.StateMachine
	srv.cache.setLastOps(snapshot.LastOps)
	return nil
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

func (srv *KVServer) applier() {
	for {
		select {
		case msg := <-srv.applyCh:
			if msg.CommandValid {
				if srv.lastApplied >= msg.CommandIndex {
					continue
				}

				var reply Reply
				op := msg.Command.(Op)
				if v, ok := srv.cache.load(op.ClientId); ok && v.Seq >= op.Seq {
					reply.Value, reply.Err = v.Value, v.Err
				} else {
					reply = srv.applyCommand(&op)
					srv.cache.store(op.ClientId, OpContext{Seq: op.Seq, Reply: reply})
				}
				srv.lastApplied = msg.CommandIndex

				if currentTerm, isLeader := srv.rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
					srv.notifier.notify(msg.CommandIndex, reply)
				}

			} else if msg.SnapshotValid {
				if srv.lastApplied >= msg.SnapshotIndex {
					continue
				}

				if err := srv.applySnapshot(msg.Snapshot); err != nil {
					panic(err)
				}

				srv.lastApplied = msg.SnapshotIndex
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
	if srv.killed() {
		return
	}

	atomic.StoreInt32(&srv.dead, 1)
	srv.rf.Kill()
	// Your code here, if desired.
	close(srv.shutdown)
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

	// You may need initialization code here.
	applyCh := make(chan raft.ApplyMsg)
	srv := &KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      applyCh,
		rf:           raft.Make(servers, me, persister, applyCh),
		stateMachine: NewMemoryKV(),
		cache:        newCache(),
		notifier:     newNotifier(),
		shutdown:     make(chan struct{}),
	}

	go srv.applier()

	return srv
}
