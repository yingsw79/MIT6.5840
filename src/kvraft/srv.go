package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Server struct {
	me      int
	Rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int

	mu sync.Mutex
	sm StateMachine

	notifier *Notifier

	lastApplied int

	shutdown chan struct{}
}

func (srv *Server) HandleRPC(args *Args, reply *Reply) {
	if !srv.Check(args, reply) {
		return
	}

	srv.Execute(*args, reply)
}

func (srv *Server) Execute(command any, reply *Reply) {
	index, _, isLeader := srv.Rf.Start(command)
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

	srv.notifier.Unregister(index)
}

func (srv *Server) maybeSnapshot(index int) {
	if !srv.needSnapshot() {
		return
	}

	s, err := srv.Snapshot()
	if err != nil {
		panic(err)
	}

	go srv.Rf.Snapshot(index, s)
}

func (srv *Server) needSnapshot() bool {
	return srv.maxraftstate != -1 && srv.Rf.RaftStateSize() >= srv.maxraftstate
}

func (srv *Server) Check(args *Args, reply *Reply) bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	return srv.sm.Check(args, reply)
}

func (srv *Server) ApplyCommand(msg any, reply *Reply) bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	return srv.sm.ApplyCommand(msg, reply)
}

func (srv *Server) Snapshot() ([]byte, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	if err := enc.Encode(srv.sm); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (srv *Server) ApplySnapshot(data []byte) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	dec := labgob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(srv.sm); err != nil {
		return err
	}
	return nil
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
				srv.ApplyCommand(msg.Command, &reply)
				srv.lastApplied = msg.CommandIndex

				if currentTerm, isLeader := srv.Rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
					srv.notifier.Notify(msg.CommandIndex, &reply)
				}

				srv.maybeSnapshot(msg.CommandIndex)

			} else if msg.SnapshotValid {
				if srv.lastApplied >= msg.SnapshotIndex {
					continue
				}

				if err := srv.ApplySnapshot(msg.Snapshot); err != nil {
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
	labgob.Register(Args{})

	applyCh := make(chan raft.ApplyMsg)
	srv := &Server{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      applyCh,
		Rf:           raft.Make(servers, me, persister, applyCh),
		sm:           stateMachine,
		notifier:     NewNotifier(),
		shutdown:     make(chan struct{}),
	}

	go srv.applier()

	return srv
}
