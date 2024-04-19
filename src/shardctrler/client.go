package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"

	"6.5840/kvraft"
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
	return &Clerk{
		servers:  servers,
		clientId: nrand(),
	}
}

func (ck *Clerk) rpc(args Op) Config {
	for {
		var reply kvraft.Reply
		if !ck.servers[ck.leaderId].Call("ShardCtrler.HandleRPC", args, &reply) ||
			reply.Err == kvraft.ErrWrongLeader || reply.Err == kvraft.ErrServerTimeout ||
			reply.Err == kvraft.ErrServerShutdown {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		ck.seq++
		if v, ok := reply.Value.(Config); ok {
			return v
		}
		return DummyConfig
	}
}

func (ck *Clerk) Query(num int) Config {
	return ck.rpc(Op{Num: num, Type: OpQuery, ClientId: ck.clientId, Seq: ck.seq})
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.rpc(Op{Servers: servers, Type: OpJoin, ClientId: ck.clientId, Seq: ck.seq})
}

func (ck *Clerk) Leave(gids []int) {
	ck.rpc(Op{GIDs: gids, Type: OpLeave, ClientId: ck.clientId, Seq: ck.seq})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.rpc(Op{Shard: shard, GID: gid, Type: OpMove, ClientId: ck.clientId, Seq: ck.seq})
}
