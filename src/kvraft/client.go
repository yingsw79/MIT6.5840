package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	clientId int64
	seq      int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// You'll have to add code here.
	return &Clerk{
		servers:  servers,
		clientId: nrand(),
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{Key: key, ClientId: ck.clientId, Seq: ck.seq}
	for {
		var reply GetReply
		if !ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply) ||
			reply.Err == ErrWrongLeader || reply.Err == ErrServerTimeout || reply.Err == ErrServerShutdown {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		ck.seq++
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, opType OpType) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Type: opType, ClientId: ck.clientId, Seq: ck.seq}
	for {
		var reply PutAppendReply
		if !ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply) ||
			reply.Err == ErrWrongLeader || reply.Err == ErrServerTimeout || reply.Err == ErrServerShutdown {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		ck.seq++
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
