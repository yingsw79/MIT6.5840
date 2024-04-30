package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
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
	return &Clerk{servers: servers, clientId: nrand()}
}

func (ck *Clerk) rpc(args *Args) string {
	for {
		var reply Reply
		if !ck.servers[ck.leaderId].Call("KVServer.HandleRPC", args, &reply) ||
			reply.Err == ErrWrongLeader || reply.Err == ErrServerTimeout ||
			reply.Err == ErrServerShutdown {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		ck.seq++
		if v, ok := reply.Value.(string); ok {
			return v
		}
		return ""
	}
}

func (ck *Clerk) Get(key string) string {
	command := Args{
		ClientId: ck.clientId, Seq: ck.seq,
		Op: Op{Key: key, Type: OpGet},
	}
	return ck.rpc(&command)
}

func (ck *Clerk) PutAppend(key string, value string, opType OpType) {
	command := Args{
		ClientId: ck.clientId, Seq: ck.seq,
		Op: Op{Key: key, Value: value, Type: opType},
	}
	ck.rpc(&command)
}

func (ck *Clerk) Put(key string, value string) { ck.PutAppend(key, value, OpPut) }

func (ck *Clerk) Append(key string, value string) { ck.PutAppend(key, value, OpAppend) }
