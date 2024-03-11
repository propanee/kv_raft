package shardctrler

import (
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
// Config.Shards 数组:
// 0 1 2 3 4 5 6 7 8 9 -- shard id
// 1 1 1 2 2 2 2 3 3 3 -- group id
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             Err = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrTimeout         = "ErrTimeout"
)

type Err string

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId int64
	SeqId    int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int
	ClientId int64
	SeqId    int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientId int64
	SeqId    int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num      int // desired config number
	ClientId int64
	SeqId    int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

const ClientRequestTimeout = 500 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Servers map[int][]string // join new GID -> servers mappings
	GIDs    []int            // leave
	Shard   int              // move
	GID     int              // move
	Num     int              // query desired config number

	OpType OperationType

	ClientId int64
	SeqId    int64
}

//func (op Op) string() string {
//	return fmt.Sprintf("%s: K %s V %s", op.OpType, op.Key, op.Value)
//}

type OpReply struct {
	Config Config
	Err    Err
}

type OperationType string

const (
	OpJoin  OperationType = "Join"
	OpLeave               = "Leave"
	OpMove                = "Move"
	OpQuery               = "Query"
)

//
//func getOpType(v string) OperationType {
//	switch v {
//	case "Put":
//		return OpPut
//	case "Append":
//		return OpAppend
//	case "Get":
//		return OpGet
//	default:
//		panic(fmt.Sprintf("Unknown operation type %s", v))
//	}
//}

type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}
