package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TempDir = "tmp"
const TaskTimeout = 10

const (
	MapTask int = iota
	ReduceTask
	WaitTask
	ExitTask
)

const (
	Unassigned int = iota
	Executing
	Finished
)

const (
	MapPhase int = iota
	ReducePhase
	FinishedPhase
)

type Task struct {
	Type   int // "Map", "Reduce", "Wait", "Exit"
	Status int // "Unassigned", "Executing", "Finished"
	Index  int // Index of the task
	// todo
	Filename  string    // File for map task
	Timestamp time.Time // Start time

	//ReduceFiles []string  // List of files for reduce task

}

type Master struct {
	nReduce int
	nMap    int

	mapTasks    []Task
	reduceTasks []Task

	Phase int

	nUnmapped  int
	nUnreduced int

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestTask(args *RequestTaskArgs, reply *TaskReply) error {
	m.mu.Lock()

	var task *Task
	switch m.Phase {
	case MapPhase:
		task = m.selectTask(m.mapTasks)
		reply.NReduce = m.nReduce
		fmt.Printf("assign map task %v\n", task.Index)
	case ReducePhase:
		task = m.selectTask(m.reduceTasks)
		reply.NMap = m.nMap
		fmt.Printf("assign reduce task %v\n", task.Index)
	case Finished:
		task = &Task{Type: ExitTask}
		fmt.Println("assign exit task")
	default:
		fmt.Println("error: wrong phase type")
		return nil
	}

	reply.Task = *task
	m.mu.Unlock()
	// todo
	if task.Type == MapTask || task.Type == ReduceTask {
		go m.waitForTask(task)
	}

	return nil
}

func (m *Master) waitForTask(task *Task) {
	if task.Type != MapTask && task.Type != ReduceTask {
		return
	}

	<-time.After(time.Second * TaskTimeout)

	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Status == Executing {
		task.Status = Unassigned
		fmt.Println("Task timeout, reset task status: ", *task)
	}
}

// get lock by caller
func (m *Master) selectTask(tasks []Task) *Task {
	// for index, task := range tasks {
	// 	if task.Status == Unassigned {
	// 		task.Status = Executing
	// 		task.Timestamp = time.Now()

	// 		return &tasks[index]
	// 	}
	// }
	for i := 0; i < len(tasks); i++ {
		if tasks[i].Status == Unassigned {
			tasks[i].Status = Executing
			tasks[i].Timestamp = time.Now()

			return &tasks[i]
		}
	}
	return &Task{Type: WaitTask}
}

func (m *Master) TaskDone(args *ReportDoneArgs, reply *ReportDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var task *Task
	if args.TaskType == MapTask {
		task = &m.mapTasks[args.TaskId]
		fmt.Printf("get report map task %v done \n", args.TaskId)
	} else if args.TaskType == ReduceTask {
		task = &m.reduceTasks[args.TaskId]
		fmt.Printf("get report reduce task %v done \n", args.TaskId)
	} else {
		fmt.Printf("Incorrect task type to report: %v\n", args.TaskType)
		return nil
	}

	// workers can only report task done if the task was not re-assigned due to timeout
	if args.Timestamp.Equal(task.Timestamp) && task.Status == Executing {
		fmt.Printf("task %v reports done.\n", *task)
		task.Status = Finished
		if args.TaskType == MapTask && m.nMap > 0 {
			m.nUnmapped--
			if m.nUnmapped == 0 {
				m.Phase = ReducePhase
			}
		} else if args.TaskType == ReduceTask && m.nReduce > 0 {
			m.nUnreduced--
			if m.nUnreduced == 0 {
				m.Phase = FinishedPhase
			}
		}
	}

	fmt.Println("master status : phase = ", m.Phase, "(unmapped, unreduced) = ", m.nUnmapped, m.nUnreduced)
	reply.Exit = m.Phase == FinishedPhase

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.Phase == FinishedPhase
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	nMap := len(files)
	m.nMap = nMap
	m.nReduce = nReduce
	m.nUnmapped = nMap
	m.nUnreduced = nReduce
	m.mapTasks = make([]Task, nMap)
	m.reduceTasks = make([]Task, nReduce)
	for i := 0; i < nMap; i++ {
		m.mapTasks[i] = Task{MapTask, Unassigned, i, files[i], time.Time{}}
		fmt.Println(m.mapTasks[i])
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = Task{ReduceTask, Unassigned, i, "", time.Time{}}
		fmt.Println(m.reduceTasks[i])
	}

	m.server()

	// clean up and create temp directory
	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}
	return &m
}
