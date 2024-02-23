package raft

import "time"

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log. Higher term, T%d<T%d",
			args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.toFollowerLocked(args.Term)
	}

	rf.resetElectionTimerLocked()
}

func (rf *Raft) startReplication(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog,
			"Lost Leader[T%d] to %s[T%d], abort ", term, rf.state, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		reply := &AppendEntriesReply{}

		go func(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)

			if !ok {
				LOG(rf.me, rf.currentTerm, DLog, "-> S%d, RPC error", peer)
				return
			}

			if reply.Term > rf.currentTerm {
				rf.toFollowerLocked(reply.Term)
				return
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
