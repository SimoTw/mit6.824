package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type STATE int

const (
	IDLE        STATE = 0
	IN_PROGRESS STATE = 1
	COMPLETED   STATE = 2
)

type TASK_TYPE int

const (
	MAP_TASK    TASK_TYPE = 0
	REDUCE_TASK TASK_TYPE = 1
)

const TIMER_LIMIT = 10

type SafeState struct {
	State STATE
	mu    sync.Mutex
}

type Task struct {
	Filename  string
	Filenames []string
	State     *SafeState // add a lock
	Timer     int        // todo: add a lock here
}

type Tasks struct {
	Tasks       []*Task
	RemainCount *SafeCounter
}

type Coordinator struct {
	// Your definitions here.
	MapTasks              *Tasks
	ReduceTasks           *Tasks
	NReduce               int
	NextWorkerId          int
	CompletedMapWorlerIds []int
}

type SafeCounter struct {
	count int
	mu    sync.Mutex
}

func (c *SafeCounter) Dec() {
	c.mu.Lock()
	c.count -= 1
	c.mu.Unlock()
}

func (c *SafeCounter) Value() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func (s *SafeState) GetState() STATE {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.State

}

func (s *SafeState) SetState(state STATE) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.State == COMPLETED {
		return
	}
	s.State = state
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTaskType() TASK_TYPE {
	if c.MapTasks.RemainCount.Value() != 0 {
		return MAP_TASK
	} else {
		return REDUCE_TASK
	}
}

func (c *Coordinator) AssignMap(args *AssignArgs, reply *AssignReply) error {
	if args.WorkerId == -1 {
		reply.WorkerId = c.NextWorkerId
		c.NextWorkerId += 1
	}
	for _, task := range c.MapTasks.Tasks {
		if task.State.GetState() == IDLE {
			reply.Filename = task.Filename
			reply.TaskType = MAP_TASK
			reply.NReduce = c.NReduce
			break
		}
	}
	return nil
}

func (c *Coordinator) AssignReduce(args *AssignArgs, reply *AssignReply) error {
	if args.WorkerId == -1 {
		reply.WorkerId = c.NextWorkerId
		c.NextWorkerId += 1
	}
	for _, task := range c.MapTasks.Tasks {
		if task.State.GetState() == IDLE {
			reply.Filenames = task.Filenames
			reply.TaskType = MAP_TASK
			reply.NReduce = c.NReduce
			break
		}
	}
	return nil
}

func (c *Coordinator) Assign(args *AssignArgs, reply *AssignReply) error {

	taskType := c.GetTaskType()
	switch taskType {
	case MAP_TASK:
		{
			c.AssignMap(args, reply)
			break
		}
	case REDUCE_TASK:
		{
			c.AssignReduce(args, reply)
			break
		}
	default:
		log.Fatalf("Unsupporte task type %v", taskType)
	}

	return nil
}

func (c *Coordinator) Complete(args *CompleteArgs, reply *CompleteReply) error {
	if args.TaskType == MAP_TASK {
		err := c.HandleMapComplete(args, reply)
		if err != nil {
			return err
		}
	} else if args.TaskType == REDUCE_TASK {
		err := c.HandleReduceComplete(args, reply)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Coordinator) HandleMapComplete(args *CompleteArgs, reply *CompleteReply) error {
	if c.GetTaskType() != MAP_TASK {
		return nil
	}

	var tasks *Tasks = c.MapTasks
	for _, task := range tasks.Tasks {
		if task.Filename == args.Filename && task.State.GetState() != COMPLETED {
			task.State.SetState(COMPLETED)
			tasks.RemainCount.Dec()
			c.CompletedMapWorlerIds = append(c.CompletedMapWorlerIds, args.WorkerId)
			break
		}
	}
	if c.MapTasks.RemainCount.Value() == 0 {
		c.InitReduceTasks()
	}
	return nil
}

func (c *Coordinator) HandleReduceComplete(args *CompleteArgs, reply *CompleteReply) error {
	return nil
}

func (c *Coordinator) Init(files []string) error {
	// caveat: task by filename without measure file size now.
	for _, filename := range files {
		task := &Task{Filename: filename, State: &SafeState{}}
		c.MapTasks.Tasks = append(c.MapTasks.Tasks, task)
	}
	c.MapTasks.RemainCount = &SafeCounter{count: len(files)}
	c.ReduceTasks.RemainCount = &SafeCounter{count: c.NReduce}
	return nil
}

func (c *Coordinator) InitReduceTasks() {
	for i := 0; i < c.NReduce; i++ {
		task := &Task{Filenames: []string{}, State: &SafeState{}, Timer: 0}
		for _, workerId := range c.CompletedMapWorlerIds {
			filename := fmt.Sprintf("mr-%v-%v", workerId, i)
			task.Filenames = append(task.Filenames, filename)
		}
		c.MapTasks.Tasks = append(c.MapTasks.Tasks, task)
	}
	c.NextWorkerId = 0
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
	ret := c.MapTasks.RemainCount.Value() == 0 && c.ReduceTasks.RemainCount.Value() == 0
	c.HandleTimeoutTasks()
	return ret
}

func (c *Coordinator) HandleTimeoutTasks() {
	// time.Sleep(time.Second)
	taskType := c.GetTaskType()
	var tasks *Tasks
	if taskType == MAP_TASK {
		tasks = c.MapTasks
	} else {
		tasks = c.ReduceTasks
	}
	for _, task := range tasks.Tasks {
		task.Timer += 1
		if task.Timer >= TIMER_LIMIT && task.State.GetState() != COMPLETED {
			task.State.SetState(IDLE)
		}
	}

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := &Tasks{}
	reduceTasks := &Tasks{}

	c := Coordinator{NReduce: nReduce, MapTasks: mapTasks, ReduceTasks: reduceTasks}

	// Your code here.
	c.Init(files)
	c.server()
	return &c
}
