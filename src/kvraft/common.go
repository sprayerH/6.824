package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
)

type Err string

type CommandRequest struct {
	Key      string
	Value    string
	OpType   string // "Put" or "Append"
	ClientId int64
	SeqId    int64
}

type CommandReply struct {
	Err   Err
	Value string
}
