package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply, succ := requestTask()

		if !succ {
			fmt.Println("Failed to contact master, worker exiting.")
			return
		}
		exit, succ := false, true
		switch reply.Task.Type {
		case ExitTask:
			fmt.Println("All tasks are done, worker exiting.")
			return
		case MapTask:
			doMap(mapf, &reply.Task, reply.NReduce)
			exit, succ = reportTaskDone(MapTask, reply.Task.Index, reply.Task.Timestamp)
		case ReduceTask:
			doReduce(reducef, &reply.Task, reply.NMap)
			exit, succ = reportTaskDone(ReduceTask, reply.Task.Index, reply.Task.Timestamp)
		case WaitTask:
			time.Sleep(time.Second)
		}

		if exit || !succ {
			fmt.Println("Master exited or all tasks done, worker exiting.")
			return
		}

	}
}

// todo 参照mrsequential.go
func doMap(mapf func(string, string) []KeyValue, task *Task, nReduce int) {
	file, err := os.Open(task.Filename)
	checkError(err, "Cannot open file %v\n", task.Filename)

	content, err := ioutil.ReadAll(file)
	checkError(err, "Cannot read file %v\n", task.Filename)
	file.Close()

	kva := mapf(task.Filename, string(content))
	writeMapOutput(kva, task.Index, nReduce)
}

// todo 参照mrsequential.go
func doReduce(reducef func(string, []string) string, task *Task, nMap int) {
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", task.Index))
	if err != nil {
		checkError(err, "Cannot list reduce files")
	}

	intermediate := []KeyValue{}
	var kv KeyValue
	for _, filePath := range files {
		file, err := os.Open(filePath)
		checkError(err, "Cannot open file %v\n", filePath)

		dec := json.NewDecoder(file)
		for dec.More() {
			err = dec.Decode(&kv)
			checkError(err, "Cannot decode from file %v\n", filePath)
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	tmpfilename := fmt.Sprintf("mr-out-%v-%v", task.Index, os.Getpid())
	ofile, err := ioutil.TempFile(TempDir, tmpfilename)
	if err != nil {
		fmt.Printf("Create output file %v failed: %v\n", tmpfilename, err)
		panic("Create file error")
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	//rename
	truename := fmt.Sprintf("mr-out-%v", task.Index)
	os.Rename(filepath.Join(ofile.Name()), truename)
	ofile.Close()
}

func writeMapOutput(kva []KeyValue, id int, nReduce int) {
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, id)
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)

	for i := 0; i < nReduce; i++ {
		// used tmp filename
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filePath)
		checkError(err, "Cannot create file %v\n", filePath)
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	// write map outputs to temp files
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		checkError(err, "Cannot encode %v to file\n", kv)
	}

	// flush file buffer to disk
	for i, buf := range buffers {
		err := buf.Flush()
		checkError(err, "Cannot flush buffer for file: %v\n", files[i].Name())
	}

	// atomically rename temp files to ensure no one observes partial files
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		checkError(err, "Cannot rename file %v\n", file.Name())
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func requestTask() (*RequestTaskReply, bool) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	succ := call("Master.RequestTask", &args, &reply)

	return &reply, succ
}

func reportTaskDone(taskType int, taskId int, timestamp time.Time) (bool, bool) {
	args := ReportDoneArgs{taskType, taskId, timestamp}
	reply := ReportDoneReply{}
	succ := call("Master.TaskDone", &args, &reply)

	return reply.Exit, succ
}

func checkError(err error, format string, v ...interface{}) {
	if err != nil {
		log.Fatalf(format, v)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
