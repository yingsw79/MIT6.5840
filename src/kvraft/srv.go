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
	StateMachine StateMachine
	LastOps      LastOps
}

type Server struct {
	me      int
	Rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int

	stateMachine StateMachine
	cache        *Cache
	notifier     *Notifier

	lastApplied int

	shutdown chan struct{}
}

func (srv *Server) HandleRPC(args IOp, reply *Reply) {
	if v, ok := srv.cache.Load(args.GetClientId()); ok && v.Seq >= args.GetSeq() {
		reply.Value, reply.Err = v.Value, v.Err
		return
	}

	index, _, isLeader := srv.Rf.Start(args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	replyCh := srv.notifier.Register(index)

	select {
	case r := <-replyCh:
		reply.Value, reply.Err = r.Value, r.Err
	case <-time.After(timeout):
		reply.Err = ErrServerTimeout
	case <-srv.shutdown:
		reply.Err = ErrServerShutdown
	}

	go srv.notifier.Unregister(index)
}

func (srv *Server) applySnapshot(data []byte) error {
	var snapshot Snapshot
	dec := labgob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&snapshot); err != nil {
		return err
	}

	srv.stateMachine = snapshot.StateMachine
	srv.cache.SetLastOps(snapshot.LastOps)
	return nil
}

func (srv *Server) maybeSnapshot(index int) {
	if !srv.needSnapshot() {
		return
	}

	s, err := srv.snapshot()
	if err != nil {
		panic(err)
	}

	go srv.Rf.Snapshot(index, s)
}

func (srv *Server) needSnapshot() bool {
	return srv.maxraftstate != -1 && srv.Rf.RaftStateSize() >= srv.maxraftstate
}

func (srv *Server) snapshot() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	snapshot := Snapshot{StateMachine: srv.stateMachine, LastOps: srv.cache.LastOps()}
	if err := enc.Encode(&snapshot); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (srv *Server) applier() {
	for {
		select {
		case msg := <-srv.applyCh:
			if msg.CommandValid {
				if srv.lastApplied >= msg.CommandIndex {
					continue
				}

				var reply Reply
				op := msg.Command.(IOp)
				if v, ok := srv.cache.Load(op.GetClientId()); ok && v.Seq >= op.GetSeq() {
					reply.Value, reply.Err = v.Value, v.Err
				} else {
					reply = srv.stateMachine.Apply(op)
					srv.cache.Store(op.GetClientId(), OpContext{Seq: op.GetSeq(), Reply: reply})
				}
				srv.lastApplied = msg.CommandIndex

				if currentTerm, isLeader := srv.Rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
					srv.notifier.Notify(msg.CommandIndex, reply)
				}

				srv.maybeSnapshot(msg.CommandIndex)

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

func (srv *Server) Kill() {
	if srv.killed() {
		return
	}

	atomic.StoreInt32(&srv.dead, 1)
	srv.Rf.Kill()
	close(srv.shutdown)
}

func (srv *Server) killed() bool {
	z := atomic.LoadInt32(&srv.dead)
	return z == 1
}

func NewServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	maxraftstate int, stateMachine StateMachine) *Server {

	applyCh := make(chan raft.ApplyMsg)
	srv := &Server{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      applyCh,
		Rf:           raft.Make(servers, me, persister, applyCh),
		stateMachine: stateMachine,
		cache:        NewCache(),
		notifier:     NewNotifier(),
		shutdown:     make(chan struct{}),
	}

	go srv.applier()

	return srv
}
