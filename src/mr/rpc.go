package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
const (
	RequestTask = iota
	FinishTask
)

type MapReduceArgs struct {
	MessageType int
	Task        Task
}

type TaskReply struct {
	Task    Task
	NReduce int
	NMap    int
}

type RequestTaskArgs struct {
	Workerid int // not used
}
type RequestTaskReply struct {
	Task    Task
	NMap    int
	NReduce int
}

type ReportDoneArgs struct {
	TaskType  int
	TaskId    int
	Timestamp time.Time
}

type ReportDoneReply struct {
	Exit bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
