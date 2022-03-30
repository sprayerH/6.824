package shardkv

import (
	"6.824/shardmaster"
)

func (kv *ShardKV) getDuplicatedCommandReply(clientId, commandId int64) (bool, CommandReply) {
	commandContext, ok := kv.lastOperations[clientId]
	if ok && commandId <= commandContext.CommandId {
		return true, *commandContext.LastReply
	} else {
		return false, CommandReply{}
	}
}

func (kv *ShardKV) canServe(shardId int) bool {
	return kv.currentConfig.Shards[shardId] == kv.gid &&
		(kv.stateMachines[shardId].Status == Serving ||
			kv.stateMachines[shardId].Status == GCing)
}

func (kv *ShardKV) getNotifyChan(index int) chan *CommandReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChans[index]
}

func (kv *ShardKV) applyLogToStateMachines(operation *CommandRequest, shardID int) CommandReply {
	var value string
	var err Err
	switch operation.OpType {
	case OpPut:
		err = kv.stateMachines[shardID].Put(operation.Key, operation.Value)
	case OpAppend:
		err = kv.stateMachines[shardID].Append(operation.Key, operation.Value)
	case OpGet:
		value, err = kv.stateMachines[shardID].Get(operation.Key)
	default:
		panic("wrong optype")
	}
	return CommandReply{err, value}
}

func (kv *ShardKV) applyConfig(config *shardmaster.Config) CommandReply {
	if config.Num == kv.currentConfig.Num+1 {
		DPrintf("{Node %v}{Group %v} updates currentConfig from %v to %v", kv.me, kv.gid, kv.currentConfig, config)
		kv.updateShardStatus(config)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *config
		return CommandReply{OK, ""}
	}
	DPrintf("{Node %v}{Group %v} rejected outdated config %v because current config num is %d", kv.me, kv.gid, config, kv.currentConfig.Num)
	return CommandReply{ErrOutdated, ""}
}

func (kv *ShardKV) applyInsertShards(shardsInfo *ShardOperationReply) CommandReply {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		DPrintf("{Node %v}{Group %v} accepts shards insertion %v when currentConfig is %v", kv.me, kv.gid, shardsInfo, kv.currentConfig)
		for shardId, shardData := range shardsInfo.Shards {
			shard := kv.stateMachines[shardId]
			if shard.Status == Pulling {
				for key, value := range shardData {
					shard.KVStore[key] = value
				}
				shard.Status = GCing
			}
		}

		for clientId, operationContext := range shardsInfo.LastOperations {
			if lastoperation, ok := kv.lastOperations[clientId]; !ok || lastoperation.CommandId < operationContext.CommandId {
				kv.lastOperations[clientId] = operationContext
			}
		}
		return CommandReply{OK, ""}
	}
	DPrintf("{Node %v}{Group %v} rejects outdated shards insertion %v when currentConfig is %v", kv.me, kv.gid, shardsInfo, kv.currentConfig)
	return CommandReply{ErrOutdated, ""}
}

func (kv *ShardKV) applyDeleteShards(shardsInfo *ShardOperationRequest) CommandReply {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		//DPrintf("{Node %v}{Group %v}'s shards status are %v before apply shards deletion %v when currentConfig is %v", kv.me, kv.gid, kv.getShardStatus(), shardsInfo, kv.currentConfig)
		for _, shardId := range shardsInfo.ShardIDs {
			shard := kv.stateMachines[shardId]
			// 本raft组已经删除远端raft组的分片
			// 通知本组成员更新状态
			if shard.Status == GCing {
				shard.Status = Serving
			} else if shard.Status == BePulling {
				kv.stateMachines[shardId] = NewShard()
			}
		}
		DPrintf("{Node %v}{Group %v}'s shards status are %v after apply shards deletion %v when currentConfig is %v", kv.me, kv.gid, kv.getShardStatus(), shardsInfo, kv.currentConfig)
		return CommandReply{OK, ""}
	}
	DPrintf("{Node %v}{Group %v}'s encounters duplicated shards deletion %v when currentConfig is %v", kv.me, kv.gid, shardsInfo, kv.currentConfig)
	return CommandReply{OK, ""}
}

func (kv *ShardKV) applyEmptyEntry() CommandReply {
	return CommandReply{OK, ""}
}
