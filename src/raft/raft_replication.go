package raft

import (
	"sort"
	"time"
)

type LogEntry struct {
	Term         int
	CommandValid bool        // if it should be applied
	Command      interface{} // the command should be applied to the state machine
	CommandIndex int         //the log entry's position in the log
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// used to probe the match point
	PreLogIndex int
	PreLogTerm  int
	Entries     []LogEntry

	// used to update the follower's commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// PreLogIndex位置的Term
	ConfilictTerm int
	// 本地log中该Term最早出现的位置
	ConfilictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// 对齐term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log. Higher term, T%d<T%d",
			args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.toFollowerLocked(args.Term)
	}
	defer rf.resetElectionTimerLocked()

	// 如果preLog没有匹配上返回false
	if args.PreLogIndex >= rf.log.size() {
		LOG(rf.me, rf.currentTerm, DLog2, "<-  S%d, Reject log, Follower log too short, len:%d < Pre%d",
			args.LeaderId, rf.log.size(), args.PreLogIndex)
		reply.ConfilictIndex = rf.log.size()
		reply.ConfilictTerm = -1
		return
	}

	if rf.log.at(args.PreLogIndex).Term != args.PreLogTerm {

		reply.ConfilictTerm = rf.log.at(args.PreLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(rf.log.at(args.PreLogIndex).Term)

		LOG(rf.me, rf.currentTerm, DLog2, "<-  S%d, Reject log, PreLog not match, "+
			"[%d]: F[T%d] != L[T%d], Conflict [%d]T%d", args.LeaderId, args.PreLogIndex,
			rf.log.at(args.PreLogIndex).Term, args.PreLogTerm, reply.ConfilictIndex, reply.ConfilictTerm)

		return
	}

	// 将args中的entries加入本地
	if len(args.Entries) > 0 {
		rf.log.appendFrom(args.PreLogIndex, args.Entries)
		LOG(rf.me, rf.currentTerm, DLog2, "Follower accept log: [%d,%d]",
			args.PreLogIndex+1, args.PreLogIndex+len(args.Entries))
		rf.persist()
	}
	reply.Success = true

	// Follower update the commit index
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d",
			rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}
}

// getMajorityLocked 获取大多数共识的index
func (rf *Raft) getMajorityLocked() int {
	tmp := make([]int, len(rf.peers))
	copy(tmp, rf.matchIndex)
	sort.Ints(tmp)
	majorityIdx := (len(tmp) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort %v, majority[%d]=%d",
		tmp, majorityIdx, tmp[majorityIdx])
	return tmp[majorityIdx]
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 只在给定的term有效
func (rf *Raft) startReplication(term int) bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[T%d] to %s[T%d], abort ", term, rf.state, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			// leader也需要更新自己的MatchIndex便于驱动后续commit
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}
		preLogIndex := rf.nextIndex[peer] - 1
		// 要发送的日志已经被截断了
		// 由于有一个mock保存了snapLastIdx的term，所以在preLogIndex处也是可以取到term的
		if preLogIndex < rf.log.snapLastIdx {
			rf.startInstallSnapshot(peer, term)
			continue
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PreLogIndex:  preLogIndex,
			PreLogTerm:   rf.log.at(preLogIndex).Term,
			Entries:      rf.log.sliceFrom(preLogIndex + 1),
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}

		go func(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.sendAppendEntries(peer, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				LOG(rf.me, rf.currentTerm, DLog, "-> S%d, RPC error", peer)
				return
			}
			// 对齐term
			if reply.Term > rf.currentTerm {
				rf.toFollowerLocked(reply.Term)
				return
			}

			//如果现在不是Leader了，就不应该在往下处理reply了
			if rf.contextLostLocked(Leader, term) {
				LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[T%d] to %s[T%d], abort replication",
					term, rf.state, rf.currentTerm)
				return
			}

			// failed: probe the lower index if the preLog not matched
			if !reply.Success {
				preNext := rf.nextIndex[peer]
				if reply.ConfilictTerm == InvalidTerm {
					rf.nextIndex[peer] = reply.ConfilictIndex
				} else {
					firstTermIndex := rf.log.firstFor(reply.ConfilictTerm)
					if firstTermIndex != InvalidIndex {
						rf.nextIndex[peer] = firstTermIndex
					} else {
						rf.nextIndex[peer] = reply.ConfilictIndex
					}
				}
				// 避免乱序
				if rf.nextIndex[peer] > preNext {
					rf.nextIndex[peer] = preNext
				}
				//idx := rf.nextIndex[peer] - 1
				//curTrem := rf.log[idx].Term
				//for idx > 0 && rf.log[idx].Term == curTrem {
				//	idx--
				//}
				//rf.nextIndex[peer] = idx + 1
				tryAgainPreTerm := InvalidTerm
				if rf.nextIndex[peer]-1 >= rf.log.snapLastIdx {
					tryAgainPreTerm = rf.log.at(rf.nextIndex[peer] - 1).Term
				}
				LOG(rf.me, rf.currentTerm, DLog, "<-  S%d, Not match in next=%d[PreT%d], try next=%d[PreT%d]",
					peer, args.PreLogIndex+1, args.PreLogTerm, rf.nextIndex[peer], tryAgainPreTerm)
				return
			}

			// success: update match/next index
			rf.matchIndex[peer] = args.PreLogIndex + len(args.Entries) // 不要用本地logs赋值，因为可能在RPC过程中有新的Log
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1

			// 将大多数共识的下标作为commitIndex，并通知apply
			majorityMatched := rf.getMajorityLocked()
			if rf.log.at(majorityMatched).Term != rf.currentTerm {
				LOG(rf.me, rf.currentTerm, DApply, "Majority %d[T%d] not match current T%d",
					majorityMatched, rf.log.at(majorityMatched).Term, rf.currentTerm)
				return
			}
			if majorityMatched > rf.commitIndex {
				LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d",
					rf.commitIndex, majorityMatched)
				rf.commitIndex = majorityMatched
				rf.applyCond.Signal()
			}

		}(peer, args, reply)
	}

	return true
}

func (rf *Raft) replicationTicker(term int) {
	for rf.killed() == false {

		ok := rf.startReplication(term)
		if !ok {
			break
		}

		time.Sleep(replicateInterval)

	}
}
