package raft

func (rf *Raft) applicationTicker() {
	for !rf.killed() {

		rf.mu.Lock()
		rf.applyCond.Wait() //一般情况下，在这里释放锁，在接收到Signal信号时再
		entries := make([]LogEntry, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.log.at(i))
		}
		rf.mu.Unlock()
		// 先锁住取数据，再放入applyCh，避免应用层apply一致占用锁
		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + 1 + i,
			}
		}
		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d,%d]", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries)
		rf.mu.Unlock()
	}

}
