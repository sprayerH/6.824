package shardmaster

import "fmt"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	OpJoin  = "Join"
	OpLeave = "Leave"
	OpMove  = "Move"
	OpQuery = "Query"
)

type CommandArgs struct {
	OpType     string
	ClientId   int64
	SequenceId int64

	// join
	Servers map[int][]string // new GID -> servers mappings
	// leaves
	GIDs []int
	// move
	Shard int
	GID   int
	//query
	Num int // desired config number
}

func (ca CommandArgs) String() string {
	switch ca.OpType {
	case OpJoin:
		return fmt.Sprintf("[%d:%d] %s<Servers:%v>", ca.ClientId, ca.SequenceId, ca.OpType, ca.Servers)
	case OpLeave:
		return fmt.Sprintf("[%d:%d] %s<GIDs:%v>", ca.ClientId, ca.SequenceId, ca.OpType, ca.GIDs)
	case OpMove:
		return fmt.Sprintf("[%d:%d] %s<Shard:%v GID:%v>", ca.ClientId, ca.SequenceId, ca.OpType, ca.Shard, ca.GID)
	case OpQuery:
		return fmt.Sprintf("[%d:%d] %s<Num:%v>", ca.ClientId, ca.SequenceId, ca.OpType, ca.Num)
	}
	panic(fmt.Sprintf("unexpected OpType %v", ca.OpType))
}

type CommandReply struct {
	Err Err

	// QueryReply
	Config Config
}

func (cr CommandReply) String() string {
	return fmt.Sprintf("{Status:%s,Config:%v}", cr.Err, cr.Config)
}
