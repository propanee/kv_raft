package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// ByKey for sorting by key.

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	wkDone := false
	for !wkDone {
		task, nReduce := getTask()
		if task == nil {
			fmt.Printf("failed to get a task\n")
			wkDone = true
		}
		//fmt.Println("nReduce", nReduce)
		switch task.TaskType {
		case MapTask:
			fmt.Printf("get a map task %d success\n", task.TaskId)
			medLocs := doMap(mapf, task, nReduce)
			finishTask(task, medLocs)
		case ReduceTask:
			fmt.Printf("get a reduce task %d success\n", task.TaskId)
			doReduce(reducef, task)
			finishTask(task, []string{})
		case WaitTask:
			fmt.Printf("waiting for tasks...\n")
			time.Sleep(1 * time.Second)
		case ExitTask:
			wkDone = true
			fmt.Printf("exiting...\n")
		}
	}
}

func finishTask(task *Task, medlocs []string) {
	args := FinishTaskArgs{
		Task:     task,
		FileLocs: medlocs,
	}
	reply := FinishTaskReply{}
	fmt.Printf("task %d finished\n", task.TaskId)
	ok := call("Coordinator.FinishTask", &args, &reply)
	if ok {
		fmt.Printf("Complete task %d success\n", task.TaskId)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func doMap(mapf func(string, string) []KeyValue, task *Task, nReduce int) []string {
	filename := task.InputFiles[0]
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))
	// 中间文件
	meds := make([][]KeyValue, nReduce)
	medLocs := make([]string, nReduce)
	for _, kv := range intermediate {
		meds[ihash(kv.Key)%nReduce] = append(meds[ihash(kv.Key)%nReduce], kv)
	}

	for i, med := range meds {
		//err = os.Mkdir("intermediates", 0755)
		//if err != nil && !os.IsExist(err) {
		//	log.Fatal(err)
		//}
		oname := fmt.Sprintf("mr-%d-%d.json", task.TaskId, i)
		medLocs[i] = oname
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range med {
			enc.Encode(kv)
		}
		ofile.Close()
	}
	return medLocs
}

func doReduce(reducef func(key string, values []string) string, task *Task) {
	var kva []KeyValue
	for _, medLoc := range task.InputFiles {
		file, _ := os.Open(medLoc)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	oname := "mr-out-" + strconv.Itoa(task.TaskId)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
}

func getTask() (*Task, int) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.PollTask", &args, &reply)
	if ok {
		return reply.Task, reply.NReduce
	} else {
		fmt.Printf("call for a Task failed!\n")
		return nil, 0
	}
}

// CallExample
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("call %s: %s", rpcname, err)
	return false
}
