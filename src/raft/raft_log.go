package raft

import (
	"6.5840/labgob"
	"fmt"
)

type RaftLog struct {
	snapLastIdx  int
	snapLastTerm int

	// contains [1, snapLastIdx]
	snapshot []byte

	// contains index [snapLastIdx +1 , snapLastIdx+len(tailLog)-1)
	// contains index snapLastIdx for mick log entry
	tailLog []LogEntry
}

func NewLog(snapLastIdx, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIdx:  snapLastIdx,
		snapLastTerm: snapLastTerm,
		snapshot:     snapshot,
	}
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})

	rl.tailLog = append(rl.tailLog, entries...)

	return rl
}

// all the functions below should be called under the protection of rf.mutex
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last snap index failed")
	}
	rl.snapLastIdx = lastIdx
	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last snap term failed")
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode tail term failed")
	}
	rl.tailLog = log

	return nil

}

func (rl *RaftLog) Persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIdx)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

// index convertion
func (rl *RaftLog) size() int {
	return rl.snapLastIdx + len(rl.tailLog)
}

func (rl *RaftLog) idx(logicIdx int) int {
	if logicIdx < rl.snapLastIdx || logicIdx > rl.size() {
		panic(fmt.Sprintf("%d is out of [%d., %d]", logicIdx, rl.snapLastIdx, rl.size()-1))
	}
	// [snapLastIdx, size()-1]
	return logicIdx - rl.snapLastIdx
}

func (rl *RaftLog) last() (index, term int) {
	idx := len(rl.tailLog) - 1
	return rl.snapLastIdx + idx, rl.tailLog[idx].Term
}

func (rl *RaftLog) at(logicIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}

func (rl *RaftLog) sliceFrom(logicIdx int) []LogEntry {
	if logicIdx >= rl.size() {
		return nil
	}
	return rl.tailLog[rl.idx(logicIdx):]
}

func (rl *RaftLog) firstFor(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return idx + rl.snapLastIdx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

func (rl *RaftLog) String() string {
	return fmt.Sprintf("Logs [%d:%d], Terms %v",
		rl.snapLastIdx, rl.size(), rl.getLogTerms())
}

func (rl *RaftLog) getLogTerms() []int {
	logTerms := make([]int, len(rl.tailLog))
	for i, lg := range rl.tailLog {
		logTerms[i] = lg.Term
	}
	return logTerms
}

func (rl *RaftLog) append(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}

func (rl *RaftLog) appendFrom(logicPrevIdx int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(logicPrevIdx)+1], entries...)
}

// do checkpoint from the applier
func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	idx := rl.idx(index)

	rl.snapLastIdx = index
	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot

	//make a new log array
	//如果直接用rl.tailLog[idx+1:]并没有改变底层的数组，无法gc
	newLog := make([]LogEntry, 0, rl.size()-rl.snapLastIdx)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}

// install snapshot from the leader
func (rl *RaftLog) installSnapshot(index, term int, snapshot []byte) {
	rl.snapLastIdx = index
	rl.snapLastTerm = term
	rl.snapshot = snapshot

	//make a new log array
	//如果直接用rl.tailLog[idx+1:]并没有改变底层的数组，无法gc
	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	rl.tailLog = newLog
}
