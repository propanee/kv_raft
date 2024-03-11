package shardctrler

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
	"time"
)

// ShardCtrler 存储一些配置信息，并不会存储用户数据，数据相对较少，可以不用实现snapshot
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	dead           int32 // set by Kill()
	lastApplied    int
	stateMachine   *CtrlerStateMachine
	notifyChans    map[int]chan *OpReply
	duplicateTable map[int64]LastOperationInfo
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		Servers:  args.Servers,
		OpType:   OpJoin,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		GIDs:     args.GIDs,
		OpType:   OpLeave,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		Shard:    args.Shard,
		GID:      args.GID,
		OpType:   OpMove,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		Num:    args.Num,
		OpType: OpQuery,
	}, &opReply)

	reply.Err = opReply.Err
	reply.Config = opReply.Config
}

func (sc *ShardCtrler) command(args Op, reply *OpReply) {
	// 判断是否是重复的请求
	sc.mu.Lock()
	if args.OpType != OpQuery && sc.requestDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复的请求，直接返回结果
		opReply := sc.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	// 调用raft，将请求存储到raft日志中
	raft.LOG(sc.me, -1, raft.DServer, "Upload %s to raft",
		args.OpType)
	index, _, isLeader := sc.rf.Start(args)
	// 如果不是leader直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//等待结果
	sc.mu.Lock()
	notifyCh := sc.makeNotifyChannel(index)
	sc.mu.Unlock()

	select {
	case result := <-notifyCh:
		raft.LOG(sc.me, -1, raft.DServer, "Apply %s SUCCESS!, Err: %s",
			args.OpType, result.Err)
		if args.OpType == OpQuery {
			reply.Config = result.Config
		}
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		sc.removeNotifyChannel(index)
		sc.mu.Unlock()
	}()

}

func (sc *ShardCtrler) requestDuplicated(clientId, seqId int64) bool {
	info, ok := sc.duplicateTable[clientId]
	return ok && seqId <= info.SeqId

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.lastApplied = 0
	sc.stateMachine = NewCtrlerStateMachine(sc.me)
	sc.notifyChans = make(map[int]chan *OpReply)
	sc.duplicateTable = make(map[int64]LastOperationInfo)

	go sc.applyTask()

	return sc
}

func (sc *ShardCtrler) applyTask() {
	for !sc.killed() {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				sc.mu.Lock()
				raft.LOG(sc.me, -1, raft.DServer, "Applying %s commandIdx %d",
					message.Command.(Op).OpType, message.CommandIndex)
				// 如果已经处理过了就直接忽略
				if message.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = message.CommandIndex
				// 取出用户的操作
				op := message.Command.(Op)
				var opReply *OpReply
				if op.OpType != OpQuery && sc.requestDuplicated(op.ClientId, op.SeqId) {
					// 如果是重复的请求，直接返回
					opReply = sc.duplicateTable[op.ClientId].Reply
				} else {
					// 将操作应用到状态机中，避免同一行命令执行多次
					raft.LOG(sc.me, -1, raft.DServer, "Applying %s to state machine",
						message.Command.(Op).OpType)
					opReply = sc.applyToStateMachine(op)
					raft.LOG(sc.me, -1, raft.DServer, "Applied %s to state machine",
						message.Command.(Op).OpType)
					if op.OpType != OpQuery {
						sc.duplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				// 将请求结果发送
				raft.LOG(sc.me, -1, raft.DServer, "Applying %s ready to send",
					message.Command.(Op).OpType)
				if _, isLeader := sc.rf.GetState(); isLeader {
					if notifyCh, ok := sc.notifyChans[message.CommandIndex]; ok {
						notifyCh <- opReply
						//raft.LOG(sc.me, -1, raft.DServer, "Apply %s SUCCESS!",
						//message.Command.(Op).string())
					}
				}

				sc.mu.Unlock()
			} else if message.SnapshotValid {
				sc.mu.Lock()
				sc.lastApplied = message.SnapshotIndex
				sc.mu.Unlock()
			}
		}

	}
}

func (sc *ShardCtrler) applyToStateMachine(op Op) *OpReply {
	var err Err
	var cfg Config
	switch op.OpType {
	case OpJoin:
		err = sc.stateMachine.Join(op.Servers)
	case OpQuery:
		cfg, err = sc.stateMachine.Query(op.Num)
	case OpLeave:
		err = sc.stateMachine.Leave(op.GIDs)
	case OpMove:
		err = sc.stateMachine.Move(op.Shard, op.GID)
	}
	return &OpReply{
		Config: cfg,
		Err:    err,
	}
}

func (sc *ShardCtrler) makeNotifyChannel(index int) chan *OpReply {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *OpReply)
	}
	raft.LOG(sc.me, -1, raft.DServer, "makeNotifyChan idx%d", index)
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) removeNotifyChannel(index int) {
	delete(sc.notifyChans, index)
}
