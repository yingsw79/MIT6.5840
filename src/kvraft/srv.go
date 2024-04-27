package kvraft

import (
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

	sm       StateMachine
	notifier *Notifier

	lastApplied int

	shutdown chan struct{}
}

func (srv *Server) HandleRPC(args *Command, reply *Reply) {
	if srv.sm.IsDuplicate(args.ClientId, args.Seq, reply) {
		return
	}

	index, _, isLeader := srv.Rf.Start(*args)
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

func (srv *Server) maybeSnapshot(index int) {
	if !srv.needSnapshot() {
		return
	}

	s, err := srv.sm.Snapshot()
	if err != nil {
		panic(err)
	}

	go srv.Rf.Snapshot(index, s)
}

func (srv *Server) needSnapshot() bool {
	return srv.maxraftstate != -1 && srv.Rf.RaftStateSize() >= srv.maxraftstate
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
				srv.sm.ApplyCommand(msg.Command, &reply)
				srv.lastApplied = msg.CommandIndex

				if currentTerm, isLeader := srv.Rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
					srv.notifier.Notify(msg.CommandIndex, &reply)
				}

				srv.maybeSnapshot(msg.CommandIndex)

			} else if msg.SnapshotValid {
				if srv.lastApplied >= msg.SnapshotIndex {
					continue
				}

				if err := srv.sm.ApplySnapshot(msg.Snapshot); err != nil {
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
	labgob.Register(Command{})

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
