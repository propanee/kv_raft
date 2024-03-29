package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	lastApplied    int
	stateMachine   *MemoryKVStateMachine
	notifyChans    map[int]chan *OpReply
	duplicateTable map[int64]LastOperationInfo
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 调用raft，将请求存储到raft日志中
	index, _, isLeader := kv.rf.Start(Op{
		Key:    args.Key,
		OpType: OpGet,
	})
	// 如果不是leader直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//等待结果 todo
	kv.mu.Lock()
	notifyCh := kv.makeNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()

}

func (kv *KVServer) requestDuplicated(clientId, seqId int64) bool {
	info, ok := kv.duplicateTable[clientId]
	return ok && seqId <= info.SeqId

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 判断是否是重复的请求
	kv.mu.Lock()
	if kv.requestDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复的请求，直接返回结果
		opReply := kv.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	// 调用raft，将请求存储到raft日志中
	index, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   getOpType(args.Op),
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	})
	// 如果不是leader直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//等待结果 todo
	kv.mu.Lock()
	notifyCh := kv.makeNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		raft.LOG(kv.me, -1, raft.DServer, "Apply %s K%s V%s SUCCESS!, Err: %s",
			args.Op, args.Key, args.Value, result.Err)
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.lastApplied = 0
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]LastOperationInfo)

	kv.restoreFromSnapshot(persister.ReadSnapshot())

	go kv.applyTask()

	return kv
}

func (kv *KVServer) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				raft.LOG(kv.me, -1, raft.DServer, "Applying %s commandIdx %d",
					message.Command.(Op).string(), message.CommandIndex)
				// 如果已经处理过了就直接忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex
				// 取出用户的操作
				op := message.Command.(Op)
				var opReply *OpReply
				if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
					// 如果是重复的请求，直接返回
					opReply = kv.duplicateTable[op.ClientId].Reply
				} else {
					// 将操作应用到状态机中，避免同一行命令执行多次
					opReply = kv.applyToStateMachine(op)
					if op.OpType != OpGet {
						kv.duplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				// 将请求结果发送
				raft.LOG(kv.me, -1, raft.DServer, "Applying %s ready to send", message.Command.(Op).string())
				if _, isLeader := kv.rf.GetState(); isLeader {
					if notifyCh, ok := kv.notifyChans[message.CommandIndex]; ok {
						notifyCh <- opReply
						raft.LOG(kv.me, -1, raft.DServer, "Apply %s SUCCESS!", message.Command.(Op).string())
					}
				}

				// 判断是否需要snapshot
				if kv.maxraftstate != -1 && kv.rf.GetRaftState() >= kv.maxraftstate {
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				kv.restoreFromSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}

	}
}

func (kv *KVServer) applyToStateMachine(op Op) *OpReply {
	var value string
	var err Err
	switch op.OpType {
	case OpGet:
		value, err = kv.stateMachine.Get(op.Key)
	case OpPut:
		err = kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		err = kv.stateMachine.Append(op.Key, op.Value)
	}
	return &OpReply{
		Value: value,
		Err:   err,
	}
}

func (kv *KVServer) makeNotifyChannel(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply)
	}
	raft.LOG(kv.me, -1, raft.DServer, "makeNotifyChan idx%d", index)
	return kv.notifyChans[index]
}

func (kv *KVServer) removeNotifyChannel(index int) {
	delete(kv.notifyChans, index)
}

func (kv *KVServer) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	_ = enc.Encode(kv.stateMachine)
	if enc.Encode(kv.stateMachine) != nil {
		panic("Failed to encode stateMachine state from snapshot")
	}

	_ = enc.Encode(kv.duplicateTable)
	raft.LOG(kv.me, -1, raft.DServer, "makeSnapshot idx%d", index)
	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *KVServer) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	var stateMachine MemoryKVStateMachine
	var dupTable map[int64]LastOperationInfo
	dec.Decode(&stateMachine)
	if dec.Decode(&stateMachine) != nil {
		panic("Failed to restore stateMachine state from snapshot")
	}
	if dec.Decode(&dupTable) != nil {
		panic("Failed to restore dupTable state from snapshot")
	}
	kv.stateMachine = &stateMachine
	kv.duplicateTable = dupTable
}
