package shardmaster

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const waitRaftTimeout time.Duration = 500 * time.Millisecond

type ShardMaster struct {
	mu      sync.RWMutex
	me      int
	dead    int32
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	maxSeqMap map[int64]CommandContext  // [clientid]seqid
	waitChans map[int]chan CommandReply //

	configStore *ConfigStore // indexed by config num
}

type CommandContext struct {
	SequenceId int64
	LastReply  CommandReply
}

type Op struct {
	// Your data here.
	//OpType     string
	CommandArgs
	// ClientId   int64
	// SequenceId int64
}

func (o Op) String() string {
	return fmt.Sprintf("[%d:%d] %s", o.ClientId, o.SequenceId, o.OpType)
}

func (sm *ShardMaster) Command(args *CommandArgs, reply *CommandReply) {
	sm.mu.RLock()
	if outdated, lastreply := sm.getDuplicatedCommandReply(args.ClientId, args.SequenceId); outdated {
		reply.Config = lastreply.Config
		reply.Err = lastreply.Err
		sm.mu.RUnlock()
		return
	}
	sm.mu.RUnlock()

	index, _, isLeader := sm.rf.Start(Op{*args})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sm.mu.Lock()
	ch := sm.getChannelL(index)
	sm.mu.Unlock()

	select {
	case rep := <-ch:
		reply.Err = rep.Err
		reply.Config = rep.Config
	case <-time.After(waitRaftTimeout):
		reply.Err = ErrWrongLeader
	}

	defer func() {
		sm.mu.Lock()
		delete(sm.waitChans, index)
		sm.mu.Unlock()
	}()
}

func (sm *ShardMaster) getChannelL(index int) chan CommandReply {
	if _, ok := sm.waitChans[index]; !ok {
		sm.waitChans[index] = make(chan CommandReply, 1)
	}
	return sm.waitChans[index]
}

func (sm *ShardMaster) applyLog() {
	for !sm.killed() {
		for message := range sm.applyCh {
			if message.CommandValid {
				//DPrintf("Server<%d> ShardMaster applych <- command index %d term %d", sm.me, message.CommandIndex, message.CommandTerm)
				sm.mu.Lock()

				op := message.Command.(Op)
				reply := CommandReply{}

				if outdated, lastreply := sm.getDuplicatedCommandReply(op.ClientId, op.SequenceId); outdated {
					reply = lastreply
				} else {
					reply = sm.applyToStore(op)
					sm.maxSeqMap[op.ClientId] = CommandContext{op.SequenceId, reply}
				}

				if currentTerm, isLeader := sm.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := sm.getChannelL(message.CommandIndex)
					ch <- reply
					//DPrintf("Server<%d> KVServer reply->chan index %d term %d", sm.me, message.CommandIndex, message.CommandTerm)
				}
				sm.mu.Unlock()
			}
		}
	}
}

func (sm *ShardMaster) applyToStore(op Op) CommandReply {
	switch op.OpType {
	case OpJoin:
		sm.configStore.Join(op.Servers)
		return CommandReply{Err: OK}
	case OpLeave:
		sm.configStore.Leave(op.GIDs)
		return CommandReply{Err: OK}
	case OpMove:
		sm.configStore.Move(op.Shard, op.GID)
		return CommandReply{Err: OK}
	case OpQuery:
		config, err := sm.configStore.Query(op.Num)
		return CommandReply{Err: err, Config: config}
	default:
		DPrintf("Server<%d> ShardMaster: wrong optype", sm.me)
		return CommandReply{}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sm.dead, 1)
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.configStore = NewConfigStore()
	sm.maxSeqMap = make(map[int64]CommandContext)
	sm.waitChans = make(map[int]chan CommandReply)

	go sm.applyLog()

	DPrintf("SVR[%d] starts", sm.me)
	return sm
}
