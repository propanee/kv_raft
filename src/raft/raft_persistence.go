package raft

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.Persist(e)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

	s := fmt.Sprintf("T%d, VotedFor: %d, %s", rf.currentTerm, rf.votedFor, rf.log.String())
	LOG(rf.me, rf.currentTerm, DPersist, "Save to disc: %v", s)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	var currentTerm int
	var votedFor int

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read currentTerm error: %v", err)
	}
	rf.currentTerm = currentTerm

	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read votedFor error: %v", err)
	}
	rf.votedFor = votedFor

	if err := rf.log.readPersist(d); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read log error: %v", err)
	}

	s := fmt.Sprintf("T%d, VotedFor: %d, %v", rf.currentTerm, rf.votedFor, rf.log.String())
	LOG(rf.me, rf.currentTerm, DPersist, "Read form disc: %s", s)

}
