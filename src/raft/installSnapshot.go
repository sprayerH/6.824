package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// caller hold lock
func (rf *Raft) handleInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.state == Leader && args.Term == rf.currentTerm {
		if reply.Term > rf.currentTerm {
			// Rules for Servers
			rf.convertToFollower(reply.Term)
			rf.persist()
		} else {
			rf.matchIndex[peer], rf.nextIndex[peer] = args.LastIncludedIndex, args.LastIncludedIndex+1
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] in (term %d) as (%v) received installSnapshot RPC {Term: %d LeaderId:%d LastIncludedIndex:%d LastIncludedTerm %d }",
		rf.me, rf.currentTerm, getStateName(rf.state), args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		rf.persist()
	}

	rf.resetElectionTimeout()
	reply.Term = rf.currentTerm

	// snapshot is outdated. raft can apply the log one by one
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}
