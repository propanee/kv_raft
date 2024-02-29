package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State string

const (
	Leader    State = "Leader"
	Follower        = "Follower"
	Candidate       = "Candidate"
)

const (
	InvalidIndex = 0
	InvalidTerm  = 0
)

const (
	electionTimeoutMin time.Duration = 150 * time.Millisecond
	electionTimeoutMax time.Duration = 300 * time.Millisecond
	replicateInterval  time.Duration = 70 * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State
	currentTerm int
	votedFor    int

	log *RaftLog

	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	applyCond   *sync.Cond

	nextIndex  []int
	matchIndex []int

	electionTimeStart time.Time
	electionTimeout   time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}

// contextLostLocked 检查rf当前的状态是否已经被改变
func (rf *Raft) contextLostLocked(state State, term int) bool {
	return !(state == rf.state && term == rf.currentTerm)
}

func (rf *Raft) toFollowerLocked(term int) {
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower, lower Term: T%d", term)
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower, For T%d->T%d", rf.state, rf.currentTerm, term)
	rf.state = Follower
	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
}

func (rf *Raft) toCandidateLocked() {
	if rf.state == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader cannot become Follower")
		return
	}

	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate[T%d]", rf.state, rf.currentTerm+1)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.state = Candidate
}

func (rf *Raft) toLeaderLocked() {
	if rf.state != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only candidate can become Leader")
		return
	}
	LOG(rf.me, rf.currentTerm, DLeader, "%s->Leader[T%d]", rf.state, rf.currentTerm)
	rf.state = Leader
	rf.votedFor = -1
	rf.persist()
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.matchIndex[peer] = 0
		rf.nextIndex[peer] = rf.log.size()
	}

}

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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2B).
	if rf.state != Leader {
		return -1, -1, false
	}
	rf.log.append(LogEntry{
		CommandValid: true,
		Command:      command,
		Term:         rf.currentTerm,
	})
	rf.persist()
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d",
		rf.log.size()-1, rf.currentTerm)

	return rf.log.size() - 1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	LOG(rf.me, rf.currentTerm, DDebug, "Crash!")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 1
	rf.votedFor = -1

	// dummy Entry，避免一些边界条件的判定
	//rf.log = append(rf.log, LogEntry{Term: InvalidTerm})
	rf.log = NewLog(InvalidIndex, InvalidTerm, nil, nil)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize the applyCh
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize sliceFrom state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start electionTicker goroutine to start elections
	go rf.electionTicker()
	go rf.applicationTicker()

	return rf
}
