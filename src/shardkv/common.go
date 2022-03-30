package shardkv

import (
	"fmt"
	"log"
	"time"

	"6.824/shardmaster"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 1 {
		log.Printf(format, a...)
	}
	return
}

const (
	ExecuteTimeout           = 500 * time.Millisecond
	ConfigureDaemonTimeout   = 100 * time.Millisecond
	MigrationDaemonTimeout   = 50 * time.Millisecond
	GCDaemonTimeout          = 50 * time.Millisecond
	EmptyEntryCheckerTimeout = 200 * time.Millisecond
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutdated    = "ErrOutdated"
	ErrTimeout     = "ErrTimeout"
	ErrNotReady    = "ErrNotReady"
)

type Err string

type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	GCing
)

func (status ShardStatus) String() string {
	switch status {
	case Serving:
		return "Serving"
	case Pulling:
		return "Pulling"
	case BePulling:
		return "BePulling"
	case GCing:
		return "GCing"
	}
	panic(fmt.Sprintf("unexpected ShardStatus %d", status))
}

type CommandType uint8

const (
	Operation     CommandType = iota
	Configuration             // update config
	InsertShards
	DeleteShards
	EmptyEntry
)

func (op CommandType) String() string {
	switch op {
	case Operation:
		return "Operation"
	case Configuration:
		return "Configuration"
	case InsertShards:
		return "InsertShards"
	case DeleteShards:
		return "DeleteShards"
	case EmptyEntry:
		return "EmptyEntry"
	}
	panic(fmt.Sprintf("unexpected CommandType %d", op))
}

const (
	OpPut    = "OpPut"
	OpAppend = "OpAppend"
	OpGet    = "OpGet"
)

type OperationContext struct {
	CommandId int64
	LastReply *CommandReply
}

func (context OperationContext) deepCopy() OperationContext {
	return OperationContext{context.CommandId, &CommandReply{context.LastReply.Err, context.LastReply.Value}}
}

// all commmand include k/v operation and shard operation
type Command struct {
	Op   CommandType
	Data interface{}
}

func (command Command) String() string {
	return fmt.Sprintf("{Type:%v,Data:%v}", command.Op, command.Data)
}

func NewOperationCommand(request *CommandRequest) Command {
	return Command{Operation, *request}
}

func NewConfigurationCommand(config *shardmaster.Config) Command {
	return Command{Configuration, *config}
}

func NewInsertShardsCommand(reply *ShardOperationReply) Command {
	return Command{InsertShards, *reply}
}

func NewDeleteShardsCommand(request *ShardOperationRequest) Command {
	return Command{DeleteShards, *request}
}

func NewEmptyEntryCommand() Command {
	return Command{EmptyEntry, nil}
}

type CommandRequest struct {
	Key       string
	Value     string
	OpType    string
	ClientId  int64
	CommandId int64
}

func (request CommandRequest) String() string {
	return fmt.Sprintf("Shard:%v,Key:%v,Value:%v,Op:%v,ClientId:%v,CommandId:%v}", key2shard(request.Key), request.Key, request.Value, request.OpType, request.ClientId, request.CommandId)
}

type CommandReply struct {
	Err   Err
	Value string
}

func (reply CommandReply) String() string {
	return fmt.Sprintf("{Err:%v,Value:%v}", reply.Err, reply.Value)
}

type ShardOperationRequest struct {
	ConfigNum int
	ShardIDs  []int
}

func (request ShardOperationRequest) String() string {
	return fmt.Sprintf("{ConfigNum:%v,ShardIDs:%v}", request.ConfigNum, request.ShardIDs)
}

type ShardOperationReply struct {
	Err            Err
	ConfigNum      int
	Shards         map[int]map[string]string
	LastOperations map[int64]OperationContext
}

func (reply ShardOperationReply) String() string {
	return fmt.Sprintf("{Err:%v,ConfigNum:%v,ShardIDs:%v,LastOperations:%v}", reply.Err, reply.ConfigNum, reply.Shards, reply.LastOperations)
}
