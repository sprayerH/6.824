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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// import "bytes"
// import "../labgob"

// some const
const ELECTION_INTERVAL = 400
const ELECTION_CHECK_TICK = 10
const HEARTBEAT_INTERVAL = 80

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
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

func getStateName(s ServerState) string {
	names := [3]string{"follower", "candidate", "leader"}
	return names[s]
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

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
	lastHeartbeat   time.Time // for leader heartbeat not used

	// persistent state
	currentTerm int
	voteFor     int
	logs        []LogEntry // lastIncludeLog saved in logs[0]
	// volatile state
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
	// // snapshot
	// lastIncludedIndex int
	// lastIncludedTerm  int

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

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeRaftState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votefor int
	var logs []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&votefor) != nil || d.Decode(&logs) != nil {
		DPrintf("[%d] in (term %d) as (%v) Error in readPersist", rf.me, rf.currentTerm, getStateName(rf.state))
	} else {
		rf.currentTerm = term
		rf.voteFor = votefor
		rf.logs = logs
		// added because of snapshot
		rf.lastApplied, rf.commitIndex = rf.logs[0].Index, rf.logs[0].Index
	}
}

// Append a new log entry into its logs,
// don't forget update nextIndex[] and matchIndex[].
// caller hold lock
func (rf *Raft) appendNewLogEntry(command interface{}) LogEntry {
	entry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   rf.getLastLog().Index + 1,
	}
	rf.logs = append(rf.logs, entry)
	rf.nextIndex[rf.me], rf.matchIndex[rf.me] = entry.Index+1, entry.Index
	rf.persist()
	return entry
}

// return the first entry of logs
func (rf *Raft) getFirstLog() LogEntry {
	return rf.logs[0]
}

// return the last entry of logs
func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

// return new slice without shard memmory
func resizeLogs(src []LogEntry) []LogEntry {
	dst := make([]LogEntry, len(src))
	copy(dst, src)
	return dst
}

// lock hold by caller. used to broadcast heartbeat and append entried to peers
// first implemention is just use heartbeat to append entries
// second implemention: true for heartbeat false for replicator( send rpc one by one )
func (rf *Raft) broadcastHeartbeat(isHeartbeat bool) {
	DPrintf("[%d] in (term %d) as (%v) broadcast heartbeat", rf.me, rf.currentTerm, getStateName(rf.state))
	lastlog := rf.getLastLog()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			//DPrintf("[%d] in (term %d) as (%v) broadcast heartbeat: lastlog.index: %d [%d] nextindex: %d", rf.me, rf.currentTerm, getStateName(rf.state), lastlog.Index, i, rf.nextIndex[i])
			// check if there are entries need to be replicated
			if lastlog.Index >= rf.nextIndex[i] || isHeartbeat {
				prevLogIndex := rf.nextIndex[i] - 1

				//should send installSnapshot RPC
				if prevLogIndex < rf.getFirstLog().Index {
					args := &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.getFirstLog().Index,
						LastIncludedTerm:  rf.getFirstLog().Term,
						Data:              rf.persister.ReadSnapshot(),
					}
					DPrintf("[%d] in (term %d) as (%v) send to [%d] installSnapshot {Term: %d, LeaderId: %d, LastIncludeIndex: %d, LastIncludeTerm: %d}",
						rf.me, rf.currentTerm, getStateName(rf.state), i, args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
					go func(peer int, args *InstallSnapshotArgs) {
						reply := &InstallSnapshotReply{}
						if rf.sendInstallSnapshot(peer, args, reply) {
							rf.mu.Lock()
							rf.handleInstallSnapshot(peer, args, reply)
							rf.mu.Unlock()
						}
					}(i, args)

					continue
				}

				// send appendEntries rpc
				firstLogIndex := rf.getFirstLog().Index
				DPrintf("[%d] in (term %d) as (%v) before send to [%d] firstlogindex: %d, prevLogIndex: %d", rf.me, rf.currentTerm, getStateName(rf.state), i, firstLogIndex, prevLogIndex)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.logs[prevLogIndex-firstLogIndex].Term,
					Entries:      make([]LogEntry, lastlog.Index-prevLogIndex),
					LeaderCommit: rf.commitIndex,
				}
				copy(args.Entries, rf.logs[prevLogIndex-firstLogIndex+1:])
				go rf.doReplicate(i, &args)
				DPrintf("[%d] in (term %d) as (%v) send to [%d] appendEntries {Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, LeaderCommit: %d}, entries lens: %+v",
					rf.me, rf.currentTerm, getStateName(rf.state), i, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
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
	DPrintf("[%d] in (term %d) as (%v) handle [%d]'s appendentries reply %+v", rf.me, rf.currentTerm, getStateName(rf.state), peer, reply)
	// check rpc delay and if rf.state has changed
	// check if it is a outdated rpc reply
	if rf.currentTerm != args.Term || rf.state != Leader {
		return
	}
	if rf.currentTerm < reply.Term {
		rf.convertToFollower(reply.Term)
		rf.persist()
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
			if rf.logs[matches[i]-rf.getFirstLog().Index].Term == rf.currentTerm {
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
		// if reply.ConflictTerm == -1 || rf.logs[reply.ConflictIndex].Term != reply.ConflictTerm {
		// 	rf.nextIndex[peer] = reply.ConflictIndex
		// } else {
		// 	for i := reply.ConflictIndex; i <= args.PrevLogIndex; i++ {
		// 		if rf.logs[i].Term != reply.ConflictTerm {
		// 			rf.nextIndex[peer] = i
		// 			break
		// 		}
		// 	}
		// }
		rf.nextIndex[peer] = reply.ConflictIndex
		if reply.ConflictTerm != -1 {
			firstLogIndex := rf.getFirstLog().Index
			i := reply.ConflictIndex
			for ; i > firstLogIndex && i <= args.PrevLogIndex; i++ {
				if rf.logs[i-firstLogIndex].Term != reply.ConflictTerm {
					rf.nextIndex[peer] = i
					break
				}
			}
		}
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
		//DPrintf("[%d] in (term %d) as (%v) Start received command: index: %d, term: %d command: %+v", rf.me, rf.currentTerm, getStateName(rf.state), index, term, command)
		return index, term, false
	}
	log := rf.appendNewLogEntry(command)
	index, term = log.Index, log.Term
	DPrintf("[%d] in (term %d) as (%v) Start received command: index: %d, term: %d log: %+v", rf.me, rf.currentTerm, getStateName(rf.state), index, term, log)
	rf.broadcastHeartbeat(false)
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
}

// only happened when args.term > rf.term.
// reset election timeout when convert from leader
func (rf *Raft) convertToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.voteFor = -1
	DPrintf("[%d] in (term %d) as (%v) become follower", rf.me, rf.currentTerm, getStateName(rf.state))
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLog().Index + 1
		rf.matchIndex[i] = 0
	}
	DPrintf("[%d] in (term %d) as (%v) become leader", rf.me, rf.currentTerm, getStateName(rf.state))
}

func (rf *Raft) HeartbeatRoutine() {
	for !rf.killed() {
		time.Sleep(time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond)
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.resetElectionTimeout()
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
	// change state to candidate
	rf.convertToCandidate()
	rf.resetElectionTimeout()
	rf.persist()
	// construct request arg
	lastLogEntry := rf.getLastLog()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
	votegotten := 1
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
				// check that current node is still in that term and state
				DPrintf("[%d] in (term %d) as (%v) get vote reply from [%d] with reply %+v ", rf.me, rf.currentTerm, getStateName(rf.state), p, reply)
				if rf.currentTerm != args.Term || rf.state != Candidate {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					rf.persist()
					return
				}
				if reply.VoteGranted {
					votegotten++
					if votegotten > len(rf.peers)/2 {
						rf.convertToLeader()
						//rf.broadcastHeartbeat(true)
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
		firstLogIndex := rf.getFirstLog().Index
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		DPrintf("[%d] in (term %d) as (%v) applies %v-%v", rf.me, rf.currentTerm, getStateName(rf.state), rf.lastApplied, rf.commitIndex)
		// todo: maybe fix up in snapshot phase cause installSnapshot would update lastapplied
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstIndex := rf.getFirstLog().Index
	if index <= firstIndex {
		return
	}
	// discard entries before index
	rf.logs = resizeLogs(rf.logs[index-firstIndex:])
	rf.logs[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
	DPrintf("[%d] in (term %d) as (%v) snapshot in index %d", rf.me, rf.currentTerm, getStateName(rf.state), index)
}

// A service wants to switch to snapshot. determin if can install snapshot
// Only do so if Raft hasn't have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]LogEntry, 1)
	} else {
		rf.logs = resizeLogs(rf.logs[lastIncludedIndex-rf.getFirstLog().Index:])
		rf.logs[0].Command = nil
	}
	rf.logs[0].Index, rf.logs[0].Term = lastIncludedIndex, lastIncludedTerm
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	// update raft state and snapshot
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
	return true
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
	rf.logs = append(rf.logs, LogEntry{0, 0, nil})
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().Unix() + int64(rf.me))
	rf.resetElectionTimeout()
	go rf.LeaderElection()
	DPrintf("[%d] peer initialized", rf.me)
	go rf.applier()

	return rf
}
