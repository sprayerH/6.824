package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int // leader commit index
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// just for lab2a now leader election
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] in (term %d) as (%v) received request appendEntries {Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, LeaderCommit: %d}",
		rf.me, rf.currentTerm, getStateName(rf.state), args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	lastlog := rf.getLastLog()
	firstlog := rf.getFirstLog()
	var mismatchIndex int
	var needPersist bool

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		DPrintf("[%d] in (term %d) as (%v) (outdated) handle appendEntries %+v", rf.me, rf.currentTerm, getStateName(rf.state), reply)
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		needPersist = true
	}

	// when term is same, change state to follower
	rf.state = Follower
	rf.resetElectionTimeout()

	// safety check when reply is delayed and follower snapshot its logs, just return false and term == 0 
	// should just return in appendEntries handler 
	if args.PrevLogIndex < firstlog.Index {
		reply.Term, reply.Success = 0, false
		return
	}

	// check prevLog
	// there is no prevlogindex
	if lastlog.Index < args.PrevLogIndex {
		reply.ConflictIndex = lastlog.Index + 1
		reply.ConflictTerm = -1
		goto End
	}
	// prevlogindex exits, but differ in term
	if rf.logs[args.PrevLogIndex-firstlog.Index].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logs[args.PrevLogIndex-firstlog.Index].Term
		reply.ConflictIndex = args.PrevLogIndex
		for i := args.PrevLogIndex; i > firstlog.Index; i-- {
			if rf.logs[i-firstlog.Index-1].Term != reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		goto End
	}

	reply.Success = true

	mismatchIndex = -1

	for i := 0; i < len(args.Entries); i++ {
		indexOfLogs := args.PrevLogIndex + 1 + i
		if indexOfLogs >= lastlog.Index+1 || rf.logs[indexOfLogs-firstlog.Index].Term != args.Entries[i].Term {
			mismatchIndex = indexOfLogs
			break
		}
	}
	// mismatch exits
	if mismatchIndex != -1 {
		// cut the tail
		rf.logs = resizeLogs(append(rf.logs[:mismatchIndex-firstlog.Index], args.Entries[mismatchIndex-args.PrevLogIndex-1:]...))
		needPersist = true
	}

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLog().Index)
		DPrintf("[%d] in (term %d) as (%v) commit index %d", rf.me, rf.currentTerm, getStateName(rf.state), rf.commitIndex)
		// do the applier or inform the applier goroutine:  applierchan <- 1
		rf.apply()
	}

	// only when recevied from leader then go here
End:
	reply.Term = rf.currentTerm
	rf.resetElectionTimeout()
	if needPersist {
		rf.persist()
	}
	DPrintf("[%d] in (term %d) as (%v) handle appendEntries %+v", rf.me, rf.currentTerm, getStateName(rf.state), reply)
}
