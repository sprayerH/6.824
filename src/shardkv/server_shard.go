package shardkv

import "6.824/shardmaster"

func (kv *ShardKV) GetShards(args *ShardOperationRequest, reply *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()
	defer DPrintf("{Node %v}{Group %v} processes PullShardsRequest %v with response %v", kv.me, kv.gid, args, reply)

	// bug? 有没有可能当前config比发送方高
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	reply.Shards = make(map[int]map[string]string)
	for _, shardID := range args.ShardIDs {
		reply.Shards[shardID] = kv.stateMachines[shardID].deepCopy()
	}

	reply.LastOperations = make(map[int64]OperationContext)
	for clientId, operation := range kv.lastOperations {
		// todo: bug maybe
		reply.LastOperations[clientId] = operation
	}

	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

func (kv *ShardKV) DeleteShards(args *ShardOperationRequest, reply *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("{Node %v}{Group %v} processes DeleteShardsRequest %v", kv.me, kv.gid, args)

	kv.mu.RLock()
	// bug? 对于发送方版本号比当前版本号低的情况，如何处理
	if kv.currentConfig.Num > args.ConfigNum {
		DPrintf("{Node %v}{Group %v}'s encounters duplicated shards deletion %v when currentConfig is %v", kv.me, kv.gid, args, kv.currentConfig)
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}

	kv.mu.RUnlock()
	var commandReply CommandReply
	kv.Execute(NewDeleteShardsCommand(args), &commandReply)
	reply.Err = commandReply.Err
}

func (kv *ShardKV) getShardStatus() []ShardStatus {
	results := make([]ShardStatus, shardmaster.NShards)
	for i := 0; i < shardmaster.NShards; i++ {
		results[i] = kv.stateMachines[i].Status
	}
	return results
}

func (kv *ShardKV) updateShardStatus(nextConfig *shardmaster.Config) {
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.currentConfig.Shards[i] != kv.gid && nextConfig.Shards[i] == kv.gid {
			gid := kv.currentConfig.Shards[i]
			if gid != 0 {
				// new shard
				kv.stateMachines[i].Status = Pulling
			}
		}
		if kv.currentConfig.Shards[i] == kv.gid && nextConfig.Shards[i] != kv.gid {
			gid := nextConfig.Shards[i]
			if gid != 0 {
				// need be deleted
				kv.stateMachines[i].Status = BePulling
			}
		}
	}
}

// 整理出上一配置的指定状态的gid2shard
// 比如当要拉取shard时 整理出状态为Pulling的对应gid的shardid
// 比如当要删除shard时 整理出状态为Gcing的对应gid的shardid
func (kv *ShardKV) getShardIDsByStatus(status ShardStatus) map[int][]int {
	gid2shardIDs := make(map[int][]int)
	for i, shard := range kv.stateMachines {
		if shard.Status == status {
			gid := kv.lastConfig.Shards[i]
			if gid != 0 {
				if _, ok := gid2shardIDs[gid]; !ok {
					gid2shardIDs[gid] = make([]int, 0)
				}
				gid2shardIDs[gid] = append(gid2shardIDs[gid], i)
			}
		}
	}
	return gid2shardIDs
}
