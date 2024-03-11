package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId int
	clientId int64
	seqId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0

	return ck
}

// Query(num) -> fetch Config # num, or latest config if num==-1.
// Query 方法的参数是一个配置编号，shardctrler 依赖于这个带编号的配置，
// 如果编号是 -1，或者大于已知的最大的编号，那么应该返回最近的一个配置。
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return reply.Config
	}
}

// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Join 方法是添加新的 Group，它的参数是一个 map，
// 存储了 Replica Group 的唯一标识 GID 到服务节点名字列表的映射关系。
func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	// Your code here.
	args.Servers = servers

	for {
		// try each known server.
		var reply JoinReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

// Leave(gids) -- delete a set of groups.
// Leave 方法的参数是一组集群中的 Group ID，表示这些 Group 退出了分布式集群。
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

// Move(shard, gid) -- hand off one shard from current owner to gid.
// Move 方法的参数是一个 shard 编号和一个 Group ID。主要是用于将一个 shard 移动到指定的 Group 中。
func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}
