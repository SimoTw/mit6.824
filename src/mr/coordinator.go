package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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
	// timer int
}



type Coordinator struct {
	// Your definitions here.
	Tasks []*Task
	NReduce int
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
	// how to keep the worker ids?
	for _, task := range c.Tasks {
		if (task.State == IDLE) {
			reply.Filename = task.Filename
			reply.TaskType = c.TaskType
			reply.NReduce = c.NReduce
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
	ret := false
	// Your code here.
	if c.TaskType != REDUCE_TASK {
		return ret
	}
	for _, task := range c.Tasks {
		if task.State != COMPLETED {
			return ret
		}
	}
	ret = true
	return ret
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

	c.server()
	return &c
}
