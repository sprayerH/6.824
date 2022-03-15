package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// import "bytes"
// import "../labgob"

// some const
const ELECTION_INTERVAL = 200
const ELECTION_CHECK_TICK = 50
const HEARTBEAT_INTERVAL = 100

func min(a, b int) int {
	if a >= b {
		return b
	} else {
		return a
	}
}

func max(a, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

func getStateName(s ServerState) string {
	names := [3]string{"follower", "candidate", "leader"}
	return names[s]
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           ServerState
	electionTimeout time.Time // for follower election timeout
	lastHeartbeat   time.Time // for leader heartbeat

	// persistent state
	currentTerm int
	voteFor     int
	logs        []LogEntry
	// volatile state
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	applyCond *sync.Cond
	applyCh   chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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
}

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

//
// example RequestVote RPC handler.
//
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
		return
	}
	rf.voteFor = args.CandidateId
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	rf.resetElectionTimeout()
}

// caller should hold lock todo: may be bug latter
func (rf *Raft) isUpToDate(term int, index int) bool {
	lastlog := rf.getLastLogEntry()
	return term > lastlog.Term || (term == lastlog.Term && index >= lastlog.Index)
}

//
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
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int // leader commit index
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// just for lab2a now leader election
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] in (term %d) as (%v) received request appendEntries {Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, LeaderCommit: %d}",
		rf.me, rf.currentTerm, getStateName(rf.state), args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	lastlog := rf.getLastLogEntry()
	var mismatchIndex int

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		DPrintf("[%d] in (term %d) as (%v) handle appendEntries %+v", rf.me, rf.currentTerm, getStateName(rf.state), reply)
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// when term is same, change state to follower
	rf.state = Follower
	rf.resetElectionTimeout()

	// 论文中的版本 日志恢复 先不考虑snapshot; todo 优化：日志快速恢复
	// check prevLog
	// there is no prevlogindex
	if lastlog.Index < args.PrevLogIndex {
		goto reply
	}
	// prevlogindex exits, but differ in term
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		goto reply
	}

	reply.Success = true

	// check if mismatch in arg.entries. if it exits, replace the part after mismatchIndex
	// if len(args.Entries) == 0 {
	// 	goto reply
	// }
	mismatchIndex = -1
	//	DPrintf("[%d] in (term %d) as (%v) recevied request appendentries with %+v in mismatch %+v", rf.me, rf.currentTerm, getStateName(rf.state), args, rf.logs)

	for i := 0; i < len(args.Entries); i++ {
		indexOfLogs := args.PrevLogIndex + 1 + i
		if indexOfLogs >= len(rf.logs) || rf.logs[indexOfLogs].Term != args.Entries[i].Term {
			mismatchIndex = indexOfLogs
			break
		}
	}
	// mismatch exits
	if mismatchIndex != -1 {
		rf.logs = append(rf.logs[:mismatchIndex], args.Entries[mismatchIndex-args.PrevLogIndex-1:]...)
	}

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogEntry().Index)
		DPrintf("[%d] in (term %d) as (%v) commit %d", rf.me, rf.currentTerm, getStateName(rf.state), rf.commitIndex)
		// do the applier or inform the applier goroutine:  applierchan <- 1
		rf.apply()
	}

	// only when recevied from leader then go here
reply:
	reply.Term = rf.currentTerm
	rf.resetElectionTimeout()
	DPrintf("[%d] in (term %d) as (%v) handle appendEntries %+v", rf.me, rf.currentTerm, getStateName(rf.state), reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// lock hold by caller. used to broadcast heartbeat and append entried to peers
// first implemention is just use heartbeat to append entries
// second implemention: true for heartbeat false for replicator( send rpc one by one )
func (rf *Raft) broadcastHeartbeat(isHeartbeat bool) {
	DPrintf("[%d] in (term %d) as (%v) broadcast heartbeat", rf.me, rf.currentTerm, getStateName(rf.state))
	lastlog := rf.getLastLogEntry()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			//DPrintf("[%d] in (term %d) as (%v) broadcast heartbeat: lastlog.index: %d [%d] nextindex: %d", rf.me, rf.currentTerm, getStateName(rf.state), lastlog.Index, i, rf.nextIndex[i])
			// check if there are entries need to be replicated
			if lastlog.Index >= rf.nextIndex[i] || isHeartbeat {
				prevLog := rf.logs[rf.nextIndex[i]-1]
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLog.Index,
					PrevLogTerm:  prevLog.Term,
					Entries:      make([]LogEntry, lastlog.Index-prevLog.Index),
					LeaderCommit: rf.commitIndex,
				}
				copy(args.Entries, rf.logs[prevLog.Index+1:])
				go rf.doReplicate(i, &args)
				// DPrintf("[%d] in (term %d) as (%v) send appendEntries arg: %+v",
				// 	rf.me, rf.currentTerm, args)
				DPrintf("[%d] in (term %d) as (%v) send to [%d] appendEntries {Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, LeaderCommit: %d}",
					rf.me, rf.currentTerm, getStateName(rf.state), i, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
			}
		}
	}
}

// the function that actually send the rpc and handle the reply
// send appendEntries rpc to peer
func (rf *Raft) doReplicate(peer int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check rpc delay and if rf.state has changed
	// check if it is a outdated rpc reply
	if rf.currentTerm != args.Term || rf.state != Leader || rf.nextIndex[peer] != args.PrevLogIndex+1 {
		return
	}

	if rf.currentTerm < reply.Term {
		rf.convertToFollower(reply.Term)
		return
	}

	if reply.Success {
		matchIndex := args.PrevLogIndex + len(args.Entries)
		// rf.matchIndex[i] may be changed during rpc procedure
		// and a duplicated rpc has procedured
		// maybe not take effective
		rf.matchIndex[peer] = max(rf.matchIndex[peer], matchIndex)
		rf.nextIndex[peer] = max(rf.nextIndex[peer], matchIndex+1)

		// check if can update commitIndex
		matches := make([]int, len(rf.peers))
		copy(matches, rf.matchIndex)
		sort.Ints(matches)
		majority := (len(rf.peers) - 1) / 2
		commited := false
		for i := majority; i >= 0 && matches[i] > rf.commitIndex; i-- {
			if rf.logs[matches[i]].Term == rf.currentTerm {
				commited = true
				rf.commitIndex = matches[i]
				break
			}
		}
		if commited {
			DPrintf("[%d] in (term %d) as (%v) commit index %d", rf.me, rf.currentTerm, getStateName(rf.state), rf.commitIndex)
			rf.apply()
		}

	} else {
		// just fallback 1 todo: should implement fast fallback
		rf.nextIndex[peer]--
	}

}

//
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return index, term, false
	}
	log := LogEntry{
		Index:   rf.getLastLogEntry().Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, log)
	rf.matchIndex[rf.me] = log.Index
	rf.nextIndex[rf.me] = log.Index + 1
	index = log.Index
	term = log.Term
	isLeader = true
	DPrintf("[%d] in (term %d) as (%v) Start received command: index: %d, term: %d", rf.me, rf.currentTerm, getStateName(rf.state), index, term)
	//rf.broadcastHeartbeat(false)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Now().Add(time.Duration(ELECTION_INTERVAL+rand.Intn(ELECTION_INTERVAL)) * time.Millisecond)
}

// reset election timeout
func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.resetElectionTimeout()
}

// only happened when args.term > rf.term.
// reset election timeout when convert from leader
func (rf *Raft) convertToFollower(newTerm int) {
	if rf.state == Leader {
		rf.resetElectionTimeout()
	}
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.voteFor = -1
	DPrintf("[%d] in (term %d) as (%v) become follower", rf.me, rf.currentTerm, getStateName(rf.state))
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogEntry().Index + 1
		rf.matchIndex[i] = 0
	}
	DPrintf("[%d] in (term %d) as (%v) become leader", rf.me, rf.currentTerm, getStateName(rf.state))
}

func (rf *Raft) getLastLogEntry() LogEntry {

	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) HeartbeatRoutine() {
	for !rf.killed() {
		time.Sleep(time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond)
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.broadcastHeartbeat(true)
		rf.mu.Unlock()
	}
}

// goroutine LeaderElection
func (rf *Raft) LeaderElection() {
	for {
		time.Sleep(time.Duration(ELECTION_CHECK_TICK) * time.Millisecond)
		rf.mu.Lock()
		//DPrintf("[%d] in (term %d) as (%v) election time out check", rf.me, rf.currentTerm)
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if time.Now().After(rf.electionTimeout) {
			if rf.state != Leader {
				DPrintf("[%d] in (term %d) as (%v) kicks off election", rf.me, rf.currentTerm, getStateName(rf.state))
				rf.KickOffElection()
			}
		}

		rf.mu.Unlock()
	}

}

func (rf *Raft) KickOffElection() {
	//rf.mu.Lock()
	// change state to candidate
	rf.convertToCandidate()
	// construct request arg
	lastLogEntry := rf.getLastLogEntry()
	//args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogEntry.Term, lastLogEntry.Index}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
	votegotten := 1
	//rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(p int) {
				//DPrintf("[%d] in (term %d) as (%v) request vote to [%d] with args %+v ", rf.me, rf.currentTerm, getStateName(rf.state), p, args)
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(p, &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// todo: need to check that current node is still in that term and state
				DPrintf("[%d] in (term %d) as (%v) get vote reply from [%d] with reply %+v ", rf.me, rf.currentTerm, getStateName(rf.state), p, reply)
				if rf.currentTerm != args.Term || rf.state != Candidate {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					return
				}
				if reply.VoteGranted {
					votegotten++
					if votegotten > len(rf.peers)/2 {
						rf.convertToLeader()
						rf.broadcastHeartbeat(true)
						go rf.HeartbeatRoutine()
					}
				}
			}(i)
		}
	}

}

// caller should hold lock
func (rf *Raft) apply() {
	rf.applyCond.Signal()
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		// todo: need to fix in snapshot phase
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1:commitIndex+1])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("[%d] in (term %d) as (%v) applies %v-%v", rf.me, rf.currentTerm, getStateName(rf.state), rf.lastApplied, rf.commitIndex)
		// todo: maybe fix up in snapshot phase cause installSnapshot would update lastapplied
		rf.lastApplied = commitIndex
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		dead:          0,
		state:         Follower,
		lastHeartbeat: time.Now(),
		currentTerm:   0,
		voteFor:       -1,
		logs:          make([]LogEntry, 0),
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		applyCh:       applyCh,
	}
	rf.logs = append(rf.logs, LogEntry{-1, 0, nil})
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().Unix())
	rf.resetElectionTimeout()
	go rf.LeaderElection()
	DPrintf("[%d] peer initialized", rf.me)
	go rf.applier()

	return rf
}
