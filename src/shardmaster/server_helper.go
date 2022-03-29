package shardmaster

func (sm *ShardMaster) getDuplicatedCommandReply(clientId, sequenceNum int64) (bool, CommandReply) {
	commandContext, ok := sm.maxSeqMap[clientId]
	if ok && sequenceNum <= commandContext.SequenceId {
		return true, commandContext.LastReply
	} else {
		return false, CommandReply{}
	}
}
