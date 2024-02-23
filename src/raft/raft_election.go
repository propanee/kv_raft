package raft

import (
	"math/rand"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CondidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//LOG(rf.me, rf.currentTerm, DVote, "Sending RequestVote")
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, higher term, T%d>T%d",
			args.CondidateId, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollowerLocked(args.Term)
	}

	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Rejected, Already voted to S%d",
			args.CondidateId, rf.votedFor)
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CondidateId
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Vote granted", args.CondidateId)

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection(term int) {
	votes := 0
	//cond := sync.NewCond(&rf.mu)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote,
			"Lost Candidate to %s, abort RequestVote", rf.state)
		return
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CondidateId:  rf.me,
			LastLogIndex: len(rf.logs) - 1,
			LastLogTerm:  0,
		}
		reply := &RequestVoteReply{}

		go func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
			//LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Ready to Send RequestVote", peer)
			ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				LOG(rf.me, rf.currentTerm, DDebug,
					"RPC error, For peer%d", peer)
				return
			}
			// 对齐term
			if reply.Term > rf.currentTerm {
				rf.toFollowerLocked(reply.Term)
				return
			}
			// 判断状态是否已经被改变了
			if rf.contextLostLocked(Candidate, term) {
				LOG(rf.me, rf.currentTerm, DVote,
					"-> S%d Lost Candidate[T%d] to %s[T%d], abort RequestVote Reply",
					peer, term, rf.state, rf.currentTerm)
				return
			}

			// 投票
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.toLeaderLocked()
					go rf.replicationTicker(term)
				}
			}
			//cond.Broadcast()
		}(peer, args, reply)

		//if votes <= len(rf.peers)/2 {
		//	cond.Wait()
		//} else {
		//	rf.toLeaderLocked()
		//	go rf.replicationTicker(term)
		//}
	}
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		if rf.state != Leader && rf.isTimeoutLocked() {
			rf.toCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
