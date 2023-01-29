package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type STATE int

const (
	IDLE STATE = 0
	IN_PROGRESS STATE = 1
	COMPLETED STATE = 2
)

type TASK_TYPE int

const (
	MAP_TASK TASK_TYPE = 0
	REDUCE_TASK TASK_TYPE = 1
)

const TIMER_LIMIT = 10

type Task struct {
	Filename string
	State STATE
	Timer int
}



type Coordinator struct {
	// Your definitions here.
	Tasks []*Task
	NReduce int
	UncompletedTasksCount int
	TaskType TASK_TYPE
	NextWorkerId int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func (c *Coordinator) Assign(args *AssignArgs, reply *AssignReply) error {
	if args.WorkerId == -1 {
		reply.WorkerId = c.NextWorkerId
		c.NextWorkerId += 1
	}
	if c.TaskType == MAP_TASK {
		for _, task := range c.Tasks {
			if (task.State == IDLE) {
				reply.Filename = task.Filename
				reply.TaskType = c.TaskType
				reply.NReduce = c.NReduce
				break
			}
		}
	} else {
		fmt.Println("get a reduce task")
		// reduce heres
	}

	return nil
}

func (c *Coordinator) Complete(args *CompleteArgs, reply *CompleteReply) error {
	for _, task := range(c.Tasks) {
		if task.Filename == args.Filename {
			task.State = COMPLETED
			c.UncompletedTasksCount -= 1
			break
		}
	}
	return nil
}

func (c *Coordinator) MakeTasks(files []string) error {
	// caveat: task by filename without measure file size now.
	for _, filename := range files {
		task := &Task{Filename:filename}
		c.Tasks = append(c.Tasks, task)
	}
	c.UncompletedTasksCount = len(files)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.TaskType == REDUCE_TASK && c.UncompletedTasksCount == 0
}

func (c *Coordinator) Timer() {
	time.Sleep(time.Second)
	for _, task := range(c.Tasks) {
		task.Timer += 1
		if task.Timer >= TIMER_LIMIT && task.State != COMPLETED{
			task.State = IDLE
		}
	}
	if c.TaskType == MAP_TASK && c.UncompletedTasksCount == 0 {
			c.TaskType = REDUCE_TASK
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{ NReduce: nReduce}

	// Your code here.
	c.MakeTasks(files)
	go c.Timer()
	c.server()
	return &c
}
