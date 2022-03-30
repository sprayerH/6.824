package shardkv

// import "../shardmaster"
import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardmaster"
)

type ShardKV struct {
	mu       sync.RWMutex
	me       int
	dead     int32
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	mck      *shardmaster.Clerk
	//masters      []*labrpc.ClientEnd
	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big
	// for snapshot
	lastApplied int

	// for config
	lastConfig    shardmaster.Config
	currentConfig shardmaster.Config

	stateMachines  map[int]*Shard
	lastOperations map[int64]OperationContext
	notifyChans    map[int]chan *CommandReply
}

func (kv *ShardKV) initStateMachines() {
	for i := 0; i < shardmaster.NShards; i++ {
		if _, ok := kv.stateMachines[i]; !ok {
			kv.stateMachines[i] = NewShard()
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	DPrintf("{Node %v}{Group %v} has been killed", kv.me, kv.gid)
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *ShardKV) Command(args *CommandRequest, reply *CommandReply) {
	kv.mu.RLock()
	//check oudated
	if outdated, lastreply := kv.getDuplicatedCommandReply(args.ClientId, args.CommandId); outdated {
		reply.Err, reply.Value = lastreply.Err, lastreply.Value
		kv.mu.RUnlock()
		return
	}

	if !kv.canServe(key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	kv.Execute(NewOperationCommand(args), reply)
}

func (kv *ShardKV) Execute(command Command, reply *CommandReply) {
	// do not hold lock to improve throughput
	// when KVServer holds the lock to take snapshot, underlying raft can still commit raft logs
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("{Node %v}{Group %v} processes Command %v ", kv.me, kv.gid, command)
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}
	DPrintf("{Node %v}{Group %v} processes Command %v reply: %+v", kv.me, kv.gid, command, reply)
	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) receiver() {
	for !kv.killed() {
		for message := range kv.applyCh {
			DPrintf("{Node %v}{Group %v} receive message: command:%v commandindex:%v snapindex:%v", kv.me, kv.gid, message.CommandValid, message.CommandIndex, message.SnapshotIndex)
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					DPrintf("{Node %v}{Group %v} discards outdated message because lastApplied is %v has been restored", kv.me, kv.gid, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var reply CommandReply
				command := message.Command.(Command)
				switch command.Op {
				case Operation:
					operation := command.Data.(CommandRequest)
					reply = kv.applyOperation(&operation)
				case Configuration:
					nextConfig := command.Data.(shardmaster.Config)
					reply = kv.applyConfig(&nextConfig)
				case InsertShards:
					shardsInfo := command.Data.(ShardOperationReply)
					reply = kv.applyInsertShards(&shardsInfo)
				case DeleteShards:
					shardsInfo := command.Data.(ShardOperationRequest)
					reply = kv.applyDeleteShards(&shardsInfo)
				case EmptyEntry:
					reply = kv.applyEmptyEntry()
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- &reply
				}

				if kv.needSnapshot() {
					//DPrintf("need snapshot statemachine because raftstatesize is %d", kv.persister.RaftStateSize())
					kv.Snapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.mu.Lock()
					if message.SnapshotIndex > kv.lastApplied {
						DPrintf("{Node %v}{Group %v} install snapshot at index %v", kv.me, kv.gid, message.SnapshotIndex)
						kv.installSnapshot(message.Snapshot)
						kv.lastApplied = message.SnapshotIndex
					}
					kv.mu.Unlock()
				}
			} else {
				panic(fmt.Sprintf("unexpected message %+v", message))
			}
		}
	}
}

func (kv *ShardKV) applyOperation(operation *CommandRequest) CommandReply {
	var reply CommandReply
	shardID := key2shard(operation.Key)
	if kv.canServe(shardID) {
		if outdated, lastreply := kv.getDuplicatedCommandReply(operation.ClientId, operation.CommandId); outdated {
			reply.Err, reply.Value = lastreply.Err, lastreply.Value
			return reply
		} else {
			reply = kv.applyLogToStateMachines(operation, shardID)
			kv.lastOperations[operation.ClientId] = OperationContext{operation.CommandId, &reply}
			return reply
		}
	}
	return CommandReply{ErrWrongGroup, ""}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(CommandRequest{})        // send by kv client
	labgob.Register(shardmaster.Config{})    // send by raftnode to other raftnode
	labgob.Register(ShardOperationRequest{}) // send by raftnode to other raftnode
	labgob.Register(ShardOperationReply{})   // send by raftnode to other raftnode

	// kv := new(ShardKV)
	// kv.me = me
	// kv.maxraftstate = maxraftstate
	// kv.make_end = make_end
	// kv.gid = gid
	// kv.masters = masters
	applyCh := make(chan raft.ApplyMsg)
	kv := &ShardKV{
		me:             me,
		dead:           0,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		make_end:       make_end,
		gid:            gid,
		mck:            shardmaster.MakeClerk(masters),
		persister:      persister,
		maxraftstate:   maxraftstate,
		lastApplied:    0,
		lastConfig:     shardmaster.Config{Groups: make(map[int][]string)},
		currentConfig:  shardmaster.Config{Groups: make(map[int][]string)},
		stateMachines:  make(map[int]*Shard),
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandReply),
	}

	// Your initialization code here.
	kv.installSnapshot(persister.ReadSnapshot())

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	go kv.receiver()
	go kv.Daemon(kv.configureRoutine, ConfigureDaemonTimeout)
	go kv.Daemon(kv.migrationRoutine, MigrationDaemonTimeout)
	go kv.Daemon(kv.gcRoutine, GCDaemonTimeout)
	go kv.Daemon(kv.checkEntryInCurrentTermRoutine, EmptyEntryCheckerTimeout)

	DPrintf("{Node %v}{Group %v} has started", me, kv.gid)

	return kv
}
