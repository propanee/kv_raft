package kvraft

import (
	"6.5840/labrpc"
	"6.5840/raft"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int // 记录leader节点
	// clientId+seqId确定唯一的command
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
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0

	return ck
}

// Get fetch the current value for a key.
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
	args := GetArgs{
		Key: key,
	}

	for {
		reply := GetReply{}
		raft.LOG(int(ck.clientId)%100, -1, raft.DClient, "-> S%d getting key %s\n", ck.leaderId, args.Key)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 请求失败换一个节点重试
			raft.LOG(int(ck.clientId)%100, -1, raft.DClient, "-> S%d getting %s Error: %s",
				ck.leaderId, args.Key, reply.Err)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		// 调用成功返回value
		raft.LOG(int(ck.clientId)%100, -1, raft.DClient,
			"-> S%d getted key %s : v %s\n", ck.leaderId, args.Key, reply.Value)
		return reply.Value
	}
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		reply := PutAppendReply{}
		raft.LOG(int(ck.clientId)%100, -1, raft.DClient, "-> S%d:%sing to Key%s, Value%s\n",
			ck.leaderId, args.Op, string(key), value)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 请求失败换一个节点重试
			raft.LOG(int(ck.clientId)%100, -1, raft.DClient, "-> S%d: %sing,Key%s, Value%s Error:%s\n",
				ck.leaderId, args.Op, string(key), value, reply.Err)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		// 调用成功返回value
		ck.seqId++
		raft.LOG(int(ck.clientId)%100, -1, raft.DClient, "-> S%d: %sed: Key%s, Value%s\n",
			ck.leaderId, args.Op, key, value)
		return

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
