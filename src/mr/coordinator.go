package mr

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskType int

// task类型
const (
	ReduceTask TaskType = iota
	MapTask
	WaitTask
	ExitTask
)

type TaskState int

// task状态
const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	TaskType   TaskType
	TaskId     int
	InputFiles []string
}

var autoIncTaskId = 0

func getAutoIncTaskId() int {
	autoIncTaskId++
	return autoIncTaskId
}

type Phase int

const (
	Mapping Phase = iota
	Reducing
	Done
)

type Coordinator struct {
	// Your definitions here.
	nReduce        int
	nMap           int
	mapTaskChan    chan *Task
	reduceTaskChan chan *Task
	phase          Phase
	taskMetas      map[int]*taskMeta
	rwmu           sync.RWMutex
}

type taskMeta struct {
	state     TaskState
	task      *Task
	outFiles  []string
	startTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) makeMapTasks(files []string) {
	for _, file := range files {
		mapTask := Task{
			TaskType:   MapTask,
			TaskId:     getAutoIncTaskId(),
			InputFiles: []string{file},
		}
		c.mapTaskChan <- &mapTask
		c.taskMetas[mapTask.TaskId] = &taskMeta{
			state:    Idle,
			task:     &mapTask,
			outFiles: make([]string, c.nReduce),
		}
		//fmt.Printf("Make a map task %d with file %s.\n",
		//	mapTask.TaskId, mapTask.InputFiles[0])
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

var waitTask = &Task{
	TaskType: WaitTask,
	TaskId:   -1,
}

var exitTask = &Task{
	TaskType: ExitTask,
	TaskId:   -1,
}

func (c *Coordinator) PollTask(args *GetTaskArgs, reply *GetTaskReply) error {
	switch c.phase {
	case Mapping:
		if len(c.mapTaskChan) > 0 {
			mapTask := <-c.mapTaskChan
			fmt.Printf("Polling a map task %d.\n", mapTask.TaskId)
			// 修改task的state

			if err := c.setTaskInProgress(mapTask); err != nil {
				reply.Task = waitTask
			}
			reply.Task = mapTask
			reply.NReduce = c.nReduce
		} else {
			c.tryMovPhase()
			reply.Task = waitTask
		}
	case Reducing:
		if len(c.reduceTaskChan) > 0 {
			reduceTask := <-c.reduceTaskChan
			//fmt.Printf("Polling a reduce task %d.\n", reduceTask.TaskId)
			// 修改task的state
			c.setTaskInProgress(reduceTask)
			reply.Task = reduceTask
		} else {
			c.tryMovPhase()
			if c.phase != Done {
				reply.Task = waitTask
			} else {
				reply.Task = exitTask
			}
		}
	}
	return nil
}

func (c *Coordinator) setTaskInProgress(task *Task) error {
	c.rwmu.Lock()
	defer c.rwmu.Unlock()
	meta, ok := c.taskMetas[task.TaskId]
	if !ok {
		return fmt.Errorf("task %d not exist", task.TaskId)
	}
	if meta.state != Idle {
		return fmt.Errorf("set task %d Inprogress failed: "+
			"already distributed with cur taskstate %d", task.TaskId, meta.state)
	}
	meta.state = InProgress
	meta.startTime = time.Now()
	fmt.Printf("set task %d Inprogress\n", task.TaskId)
	return nil
}

func (c *Coordinator) resetTaskIdle(task *Task) error {
	//c.rwmu.Lock()
	//defer c.rwmu.Unlock()
	meta, ok := c.taskMetas[task.TaskId]
	if !ok {
		return fmt.Errorf("task %d not exist", task.TaskId)
	}
	switch meta.state {
	case Completed:
		return fmt.Errorf("task %d is already Completed, "+
			"can not reset to idle", task.TaskId)
	case Idle:
		return fmt.Errorf("task %d is Idle, can not reset to idle", task.TaskId)
	}
	meta.state = Idle
	fmt.Printf("reset task %d Idle\n", task.TaskId)
	return nil
}

// FinishTask worker使用这个rpc告知task完成
func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	task := args.Task
	fmt.Printf("finishing task %d", task.TaskId)
	c.rwmu.Lock()
	defer c.rwmu.Unlock()
	meta := c.taskMetas[task.TaskId]
	if meta.state == Completed {
		return fmt.Errorf("the task %d is already Completed", task.TaskId)
	}
	meta.state = Completed
	if task.TaskType == MapTask {
		sort.Strings(args.FileLocs)
		meta.outFiles = args.FileLocs
	}
	fmt.Printf("Task %d is done\n", task.TaskId)
	return nil
}

func (c *Coordinator) makeReduceTasks() {
	fmt.Printf("Making reduce tasks\n")
	rFiles := make([][]string, c.nReduce)
	for i := range rFiles {
		rFiles[i] = make([]string, c.nMap)
	}
	for mi, meta := range c.taskMetas {
		for ri, medfile := range meta.outFiles {
			rFiles[ri][mi-1] = medfile
		}
	}
	for i := 0; i < c.nReduce; i++ {
		reduceTask := &Task{
			TaskType:   ReduceTask,
			TaskId:     getAutoIncTaskId(),
			InputFiles: rFiles[i],
		}
		c.reduceTaskChan <- reduceTask
		fmt.Printf("Make a reduce task %d\n", reduceTask.TaskId)
		c.taskMetas[reduceTask.TaskId] = &taskMeta{
			state: Idle,
			task:  reduceTask,
		}
	}
}

// checkTasksDone 检查所有的task是否都被解决了
// map阶段，task都被解决，可以进入reduce阶段
// reduce阶段，task都被解决，可以Done
func (c *Coordinator) checkTasksDone() bool {

	for _, meta := range c.taskMetas {
		if meta.state != Completed {
			return false
		}
	}
	return true
}

// tryMovPhase 尝试进入下一阶段
func (c *Coordinator) tryMovPhase() bool {
	fmt.Printf("trying move to next phase\n")
	c.rwmu.Lock()
	defer c.rwmu.Unlock()
	//fmt.Printf("trying move to next phase")
	if tdone := c.checkTasksDone(); !tdone {
		return false
	}
	switch c.phase {
	case Mapping:
		c.makeReduceTasks()
		c.phase = Reducing
		fmt.Printf("Maps are Done, move to reduce phase.\n")
	case Reducing:
		fmt.Printf("Reduces are Done.\n")
		c.phase = Done
	}
	return true
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.rwmu.RLock()
	defer c.rwmu.RUnlock()
	if c.phase == Done {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:        nReduce,
		nMap:           len(files),
		mapTaskChan:    make(chan *Task, len(files)),
		reduceTaskChan: make(chan *Task, nReduce),
		phase:          Mapping,
		taskMetas:      make(map[int]*taskMeta, len(files)+nReduce),
	}
	c.makeMapTasks(files)
	// Your code here.

	go c.crashDetector()

	c.server()
	return &c
}

func (c *Coordinator) crashDetector() {
	for {
		c.rwmu.RLock()
		if c.phase == Done {
			c.rwmu.RUnlock()
			break
		}
		var crashTasks []*Task
		for _, meta := range c.taskMetas {
			if meta.state == InProgress &&
				time.Since(meta.startTime) >= 10*time.Second {
				crashTasks = append(crashTasks, meta.task)
			}
		}

		c.rwmu.RUnlock()
		if len(crashTasks) == 0 {
			time.Sleep(time.Second)
			continue
		}
		for _, ct := range crashTasks {
			c.rwmu.Lock()
			fmt.Printf("task %d is crashed\n", ct.TaskId)
			if err := c.resetTaskIdle(ct); err != nil {
				fmt.Println(err)
				c.rwmu.Unlock()
				continue
			}
			if ct.TaskType == MapTask {
				c.mapTaskChan <- ct
			} else if ct.TaskType == ReduceTask {
				c.reduceTaskChan <- ct
			}
			c.rwmu.Unlock()
		}
		time.Sleep(time.Second)
	}
}
