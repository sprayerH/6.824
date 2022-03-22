package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] in (term %d) as (%v) received voteRequest with %+v", rf.me, rf.currentTerm, getStateName(rf.state), args)
	// current term bigger than arg.term or same term but has voted for other candidate
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	// check up-to-date todo: maybe some bug here because dont check if logs is empty
	if !rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		rf.persist()
		return
	}
	rf.voteFor = args.CandidateId
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	rf.resetElectionTimeout()
	rf.persist()
}

// caller should hold lock todo: may be bug latter
func (rf *Raft) isUpToDate(term int, index int) bool {
	lastlog := rf.getLastLog()
	return term > lastlog.Term || (term == lastlog.Term && index >= lastlog.Index)
}
