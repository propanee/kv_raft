package raft

import "fmt"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.log.doSnapshot(index, snapshot)
	rf.persist()

}

// Invoked by leader to send chunks of a snapshot to a follower.
// Leaders always send chunks in order.
// 这里假设可以通过一次rpc全部发送过去
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

func (args InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader %d, T%d,LastIncluded: [%d]T%d",
		args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type InstallSnapshotReply struct {
	// Your data here (2A).
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, RecvSnapshot: %s",
		args.LeaderId, args.String())

	reply.Term = rf.currentTerm

	//对齐term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap. Higher term, T%d<T%d",
			args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	// = 意味着peer为Candidate时也要变成Follower
	if args.Term >= rf.currentTerm {
		rf.toFollowerLocked(args.Term)
	}

	// 检查是不是本地已经有了RPC中的 snapshot
	if rf.log.snapLastIdx >= args.LastIncludedIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap. Aready installed:%d>=%d",
			args.LeaderId, rf.log.snapLastIdx, args.LastIncludedIndex)
		return
	}

	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)

	rf.persist()

	rf.snapPending = true
	rf.applyCond.Signal()

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) startInstallSnapshot(server, term int) {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.snapLastIdx,
		LastIncludedTerm:  rf.log.snapLastTerm,
		Snapshot:          rf.log.snapshot,
	}
	reply := &InstallSnapshotReply{}

	LOG(rf.me, rf.currentTerm, DSnap, "->S%d, InstallSnapshot", server)

	go func(server, term int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
		ok := rf.sendInstallSnapshot(server, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DSnap, "-> S%d, RPC error", server)
			return
		}
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, InstallSnapshot Reply:T%d", server, reply.Term)
		// 对齐term
		if reply.Term > rf.currentTerm {
			rf.toFollowerLocked(reply.Term)
			return
		}

		//如果现在不是Leader了，就不应该在往下处理reply了
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DSnap, "Lost Leader[T%d] to %s[T%d], abort InstallSnapshot",
				term, rf.state, rf.currentTerm)
			return
		}

		// update match/next index
		// 为了防止有rpc回来的比较晚，需要在大于当前match的时候才更新
		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}

		// 注意这里snapshot一定是提交过的，所以这里不要再改变commitIndex
	}(server, term, args, reply)

}
