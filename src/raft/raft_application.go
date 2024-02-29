package raft

func (rf *Raft) applicationTicker() {
	for !rf.killed() {

		rf.mu.Lock()
		rf.applyCond.Wait() //一般情况下，在这里释放锁，在接收到Signal信号时再
		snapPendingApply := rf.snapPending
		entries := make([]LogEntry, 0)
		if !snapPendingApply {

			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				entries = append(entries, rf.log.at(i))
			}
		}

		rf.mu.Unlock()
		// 先锁住取数据，再放入applyCh，避免应用层apply一致占用锁
		if !snapPendingApply {
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + 1 + i,
				}
			}
		} else {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotTerm:  rf.log.snapLastTerm,
				SnapshotIndex: rf.log.snapLastIdx,
			}
		}

		rf.mu.Lock()
		if !snapPendingApply {
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d,%d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Apply snapshot for [0,%d]", rf.log.snapLastIdx)
			rf.lastApplied = rf.log.snapLastIdx
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false
		}

		rf.mu.Unlock()
	}

}
