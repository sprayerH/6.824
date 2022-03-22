package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const waitRaftTimeout time.Duration = 500 * time.Millisecond

type Op struct {
	Key      string
	Value    string
	OpType   string // "Put" or "Append"
	ClientId int64
	SeqId    int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	maxSeqMap   map[int64]int64           // [clientid]seqid
	waitChans   map[int]chan CommandReply //
	kvStore     map[string]string
	lastApplied int
}

func (kv *KVServer) getChannelL(index int) chan CommandReply {
	if _, ok := kv.waitChans[index]; !ok {
		kv.waitChans[index] = make(chan CommandReply, 1)
	}
	return kv.waitChans[index]
}

func (kv *KVServer) isDuplicatedL(clientId, seqId int64) bool {
	maxSeq, ok := kv.maxSeqMap[clientId]
	return ok && maxSeq >= seqId
}

func (kv *KVServer) CommandRequest(args *CommandRequest, reply *CommandReply) {
	//defer DPrintf("Server-{%v} processes request %v with reply %v", kv.me, args, reply)
	kv.mu.Lock()
	// check duplicated and outdated write request and just return ok
	if args.OpType != OpGet && kv.isDuplicatedL(args.ClientId, args.SeqId) {
		DPrintf("Server<%d> KVServer<-[%d:%d] outdatedCommand", kv.me, args.ClientId, args.SeqId)
		reply.Err, reply.Value = OK, ""
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   args.OpType,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	})
	if !isLeader {
		//DPrintf("Server<%d> KVServer<-[%d:%d] not Leader", kv.me, args.ClientId, args.SeqId)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getChannelL(index)
	kv.mu.Unlock()

	DPrintf("Server<%d> KVServer<-[%d:%d] wait chan %d", kv.me, args.ClientId, args.SeqId, index)
	select {
	case r := <-ch:
		DPrintf("Server<%d> KVServer<-[%d:%d] get from chan index %d", kv.me, args.ClientId, args.SeqId, index)
		reply.Err, reply.Value = r.Err, r.Value
		return
	case <-time.After(waitRaftTimeout):
		// maybe ErrTimeout is better
		//DPrintf("Server<%d> KVServer<-[%d:%d] timeout", kv.me, args.ClientId, args.SeqId)
		reply.Err = ErrWrongLeader
	}
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChans, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) applyLog() {
	for !kv.killed() {
		for message := range kv.applyCh {
			//case message := <-kv.applyCh:
			if message.CommandValid {
				DPrintf("Server<%d> KVServer applych <- command index %d term %d", kv.me, message.CommandIndex, message.CommandTerm)
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				op := message.Command.(Op)
				//DPrintf("Server<%d> KVServer process op %v", kv.me, op)
				reply := CommandReply{OK, ""}
				if op.OpType != OpGet && kv.isDuplicatedL(op.ClientId, op.SeqId) {
					reply = CommandReply{OK, ""}
				} else {
					if op.OpType != OpGet {
						kv.maxSeqMap[op.ClientId] = op.SeqId
					}
					switch op.OpType {
					case OpPut:
						kv.kvStore[op.Key] = op.Value
						//DPrintf("Server<%d> KVServer Put value", kv.me, op.Value, op.Key)
					case OpAppend:
						kv.kvStore[op.Key] += op.Value
						//DPrintf("Server<%d> KVServer Append value", kv.me, op.Value, op.Key)
					case OpGet:
						if value, ok := kv.kvStore[op.Key]; ok {
							reply.Value = value
						} else {
							reply.Err = ErrNoKey
						}
					}
				}

				// follower should not notify
				// 有没有可能server apply之后就挂了/退化成了follower
				// alternative: 把start中获得的index和term来作对比 如果一致才能返回给
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getChannelL(message.CommandIndex)
					ch <- reply
					DPrintf("Server<%d> KVServer reply->chan index %d term %d", kv.me, message.CommandIndex, message.CommandTerm)
				}

				// check if need snapshot
				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
					kv.Snapshot(kv.lastApplied)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid {
				DPrintf("Server<%d> KVServer applych <- snapshot index %d term %d", kv.me, message.SnapshotIndex, message.SnapshotTerm)
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					DPrintf("Server<%d> KVServer install snapshot at index %d", kv.me, message.SnapshotIndex)
					kv.mu.Lock()
					if message.SnapshotIndex > kv.lastApplied {
						kv.installSnapshot(message.Snapshot)
						kv.lastApplied = message.SnapshotIndex
					}
					kv.mu.Unlock()
				}
			}
		}
	}
}

// caller hold lock
func (kv *KVServer) Snapshot(index int) {
	DPrintf("Server<%d> KVServer snapshot at before index %d", kv.me, index)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.maxSeqMap)
	e.Encode(kv.lastApplied)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

// caller hold lock
func (kv *KVServer) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvStore map[string]string
	var maxseqMap map[int64]int64
	var lastApplied int

	if d.Decode(&kvStore) != nil || d.Decode(&maxseqMap) != nil ||
		d.Decode(&lastApplied) != nil {
		DPrintf("error to read the snapshot data")
	} else {
		kv.kvStore = kvStore
		kv.maxSeqMap = maxseqMap
		kv.lastApplied = lastApplied
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.maxSeqMap = make(map[int64]int64)
	kv.waitChans = make(map[int]chan CommandReply)
	kv.kvStore = make(map[string]string)
	kv.lastApplied = 0

	kv.installSnapshot(kv.persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.applyLog()

	return kv
}
