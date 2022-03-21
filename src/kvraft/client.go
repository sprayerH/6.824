package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

const retryTimeout = 10

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId   int64
	seqId      int64
	lastServer int
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
		clientId:   nrand(),
		seqId:      0,
		lastServer: 0,
	}
}

func (ck *Clerk) RequestCommand(key string, value string, op string) string {
	args := CommandRequest{
		Key:      key,
		Value:    value,
		OpType:   op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	for {
		reply := CommandReply{}
		ok := ck.servers[ck.lastServer].Call("KVServer.CommandRequest", &args, &reply)
		//DPrintf("Client[%v] SeqId[%v] leaderServer: %v GetReply: %+v", ck.clientId, ck.seqId, ck.lastServer, reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			DPrintf("Client[%v] SeqId[%v] success leaderServer: %v GetReply: %+v", ck.clientId, ck.seqId, ck.lastServer, reply)
			ck.seqId++
			return reply.Value
		} else {
			ck.lastServer = (ck.lastServer + 1) % len(ck.servers)
			DPrintf("Client[%v] SeqId[%v] leaderServer: %v GetReply: %+v new server: %d", ck.clientId, ck.seqId, ck.lastServer, reply, ck.lastServer)
			time.Sleep(retryTimeout * time.Millisecond)
		}
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	return ck.RequestCommand(key, "", OpGet)
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.RequestCommand(key, value, op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
