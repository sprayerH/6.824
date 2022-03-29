package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId   int64
	clientId   int64
	sequenceId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:    servers,
		leaderId:   0,
		clientId:   nrand(),
		sequenceId: 0,
	}
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandArgs{
		OpType:     OpQuery,
		Num:        num,
		ClientId:   ck.clientId,
		SequenceId: ck.sequenceId,
	}
	return ck.Command(args)
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandArgs{
		OpType:     OpJoin,
		Servers:    servers,
		ClientId:   ck.clientId,
		SequenceId: ck.sequenceId,
	}
	ck.Command(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &CommandArgs{
		OpType:     OpLeave,
		GIDs:       gids,
		ClientId:   ck.clientId,
		SequenceId: ck.sequenceId,
	}
	ck.Command(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{
		OpType:     OpMove,
		Shard:      shard,
		GID:        gid,
		ClientId:   ck.clientId,
		SequenceId: ck.sequenceId,
	}
	ck.Command(args)
}

func (ck *Clerk) Command(args *CommandArgs) Config {
	for {
		reply := &CommandReply{}
		DPrintf("Client[%d] send command(type: %s) to server[%d]", ck.clientId, args.OpType, ck.leaderId)
		ok := ck.servers[ck.leaderId].Call("ShardMaster.Command", args, reply)
		if !ok || reply.Err != OK {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			time.Sleep(100 * time.Millisecond)
			continue
		}
		DPrintf("Client[%d] received reply of command(type: %s) from server[%d]", ck.clientId, args.OpType, ck.leaderId)
		ck.sequenceId++
		return reply.Config
	}
}
