package raft

import (
	"math/rand"
	"sync"
	"time"
)

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

type msgHandler interface {
	handle()
}

type rpcMsgHandler struct {
	args    any
	reply   any
	handler func(any, any)
	done    chan struct{}
}

func (h rpcMsgHandler) handle() {
	h.handler(h.args, h.reply)
	close(h.done)
}

type rpcReplyMsgHandler struct {
	reply   any
	handler *Raft
}

func (h rpcReplyMsgHandler) handle() {
	h.handler.step(h.reply)
}

// type serviceMsgHandler struct {
// 	msg     *ApplyMsg
// 	handler *Raft
// }

// func (h serviceMsgHandler) handle() {
// 	h.handler.step(h.msg)
// 	go func() {
// 		select {
// 		case h.handler.applyCh <- *h.msg:
// 		case <-h.handler.done:
// 		}
// 	}()
// }

// type tracker struct {
// }
