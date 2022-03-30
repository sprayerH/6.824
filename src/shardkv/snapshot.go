package shardkv

import (
	"bytes"

	"6.824/labgob"
	"6.824/shardmaster"
)

// caller hold lock
func (kv *ShardKV) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate
}

// caller hold lock
func (kv *ShardKV) Snapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachines)
	e.Encode(kv.lastOperations)
	e.Encode(kv.currentConfig)
	e.Encode(kv.lastConfig)
	e.Encode(kv.lastApplied)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

// caller hold lock
func (kv *ShardKV) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		kv.initStateMachines()
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachines map[int]*Shard
	var lastOperations map[int64]OperationContext
	var currentConfig shardmaster.Config
	var lastConfig shardmaster.Config
	var lastApplied int

	if d.Decode(&stateMachines) != nil || d.Decode(&lastOperations) != nil ||
		d.Decode(&currentConfig) != nil || d.Decode(&lastConfig) != nil ||
		d.Decode(&lastApplied) != nil {
		DPrintf("error to read the snapshot data")
	} else {
		kv.stateMachines = stateMachines
		kv.lastOperations = lastOperations
		kv.currentConfig = currentConfig
		kv.lastConfig = lastConfig
		kv.lastApplied = lastApplied
	}
}
