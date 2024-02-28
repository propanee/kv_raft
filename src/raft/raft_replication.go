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

	// 如果preLog没有匹配上返回false
	if args.PreLogIndex >= len(rf.logs) {
		LOG(rf.me, rf.currentTerm, DLog2, "<-  S%d, Reject log, Follower log too short, len:%d < Pre%d",
			args.LeaderId, len(rf.logs), args.PreLogIndex)
		return
	}

	if rf.logs[args.PreLogIndex].Term != args.PreLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<-  S%d, Reject log, PreLog not match, [%d]: F[T%d] != L[T%d]",
			args.LeaderId, args.PreLogIndex, rf.logs[args.PreLogIndex].Term, args.PreLogTerm)
		return
	}

	// 将args中的entries加入本地
	if len(args.Entries) > 0 {
		rf.logs = append(rf.logs[:args.PreLogIndex+1], args.Entries...)
		LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs: [%d,%d]",
			args.PreLogIndex+1, args.PreLogIndex+len(args.Entries))

	}
	reply.Success = true

	// Follower update the commit index
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d",
			rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}

	rf.resetElectionTimerLocked()
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
			rf.matchIndex[peer] = len(rf.logs) - 1
			rf.nextIndex[peer] = len(rf.logs)
			continue
		}
		preLogIndex := rf.nextIndex[peer] - 1
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PreLogIndex:  preLogIndex,
			PreLogTerm:   rf.logs[preLogIndex].Term,
			Entries:      rf.logs[preLogIndex+1:],
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}

		go func(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
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
				LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[T%d] to %s[T%d], abort ",
					term, rf.state, rf.currentTerm)
				return
			}

			// failed: probe the lower index if the preLog not matched
			if !reply.Success {
				idx := rf.nextIndex[peer] - 1
				curTrem := rf.logs[idx].Term
				for idx > 0 && rf.logs[idx].Term == curTrem {
					idx--
				}
				rf.nextIndex[peer] = idx + 1
				LOG(rf.me, rf.currentTerm, DLog, "Not match at %d with S%d in next=%d[PreT%d], try next=%d[PreT%d]",
					term, peer, args.PreLogIndex+1, args.PreLogTerm, rf.nextIndex[peer], rf.logs[idx].Term)
				return
			}

			// success: update match/next index
			rf.matchIndex[peer] = args.PreLogIndex + len(args.Entries) // 不要用本地logs赋值，因为可能在RPC过程中有新的Log
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1

			// 将大多数共识的下标作为commitIndex，并通知apply
			majorityMatched := rf.getMajorityLocked()
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
