package shardkv

import (
	"sync"
	"time"
)

func (kv *ShardKV) Daemon(routine func(), timeout time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			routine()
		}
		time.Sleep(timeout)
	}
}

// get new config only when reconfig is finished
// todo: maybe can use condition variable
func (kv *ShardKV) configureRoutine() {
	canPerformNextConfig := true
	kv.mu.RLock()
	for _, shard := range kv.stateMachines {
		if shard.Status != Serving {
			canPerformNextConfig = false
			break
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.RUnlock()
	if canPerformNextConfig {
		nextConfig := kv.mck.Query(currentConfigNum + 1)
		if nextConfig.Num == currentConfigNum+1 {
			DPrintf("{Node %v}{Group %v} fetches latest configuration %v when currentConfigNum is %v", kv.me, kv.gid, nextConfig, currentConfigNum)
			kv.Execute(NewConfigurationCommand(&nextConfig), &CommandReply{})
		}
	}
}

func (kv *ShardKV) migrationRoutine() {
	kv.mu.RLock()
	gid2shardIDs := kv.getShardIDsByStatus(Pulling)
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		DPrintf("{Node %v}{Group %v} starts a PullTask to get shards %v from group %v when config is %v", kv.me, kv.gid, shardIDs, gid, kv.currentConfig)
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			pullTaskRequest := ShardOperationRequest{configNum, shardIDs}
			for _, server := range servers {
				var pullTaskResponse ShardOperationReply
				srv := kv.make_end(server)
				if srv.Call("ShardKV.GetShards", &pullTaskRequest, &pullTaskResponse) && pullTaskResponse.Err == OK {
					DPrintf("{Node %v}{Group %v} gets a PullTaskResponse %v and tries to commit it when currentConfigNum is %v", kv.me, kv.gid, pullTaskResponse, configNum)
					kv.Execute(NewInsertShardsCommand(&pullTaskResponse), &CommandReply{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIDs)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

func (kv *ShardKV) gcRoutine() {
	kv.mu.RLock()
	gid2shardIDs := kv.getShardIDsByStatus(GCing)
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		DPrintf("{Node %v}{Group %v} starts a GCTask to delete shards %v in group %v when config is %v", kv.me, kv.gid, shardIDs, gid, kv.currentConfig)
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			gcTaskRequest := ShardOperationRequest{configNum, shardIDs}
			for _, server := range servers {
				var gcTaskResponse ShardOperationReply
				srv := kv.make_end(server)
				if srv.Call("ShardKV.DeleteShards", &gcTaskRequest, &gcTaskResponse) && gcTaskResponse.Err == OK {
					DPrintf("{Node %v}{Group %v} deletes shards %v in remote group successfully when currentConfigNum is %v", kv.me, kv.gid, shardIDs, configNum)
					kv.Execute(NewDeleteShardsCommand(&gcTaskRequest), &CommandReply{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIDs)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

func (kv *ShardKV) checkEntryInCurrentTermRoutine() {
	if !kv.rf.HasLogInCurrentTerm() {
		kv.Execute(NewEmptyEntryCommand(), &CommandReply{})
	}
}
