package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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

// todo: migrate timer to a worker record
type WorkerRecond struct {
	Timer    int // discussion: could timer have a race?
	WorkerId int
}

type Task struct {
	Id              int
	Filename        string
	Filenames       []string
	State           *SafeState // add a lock
	Timer           int        // todo: add a lock here
	OutputFilenames []string
	OutputFilename  string
}

type MapTasks struct {
	Tasks       []*Task
	RemainCount *SafeCounter
}

type ReduceTasks struct {
	Tasks       []*Task
	RemainCount *SafeCounter
}

type Coordinator struct {
	// Your definitions here.
	MapTasks    *MapTasks
	ReduceTasks *ReduceTasks
	NReduce     int
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

func (c *Coordinator) GetTaskType() TASK_TYPE {
	if c.MapTasks.RemainCount.Value() != 0 {
		return MAP_TASK
	} else {
		return REDUCE_TASK
	}
}

func (c *Coordinator) AssignMap(args *AssignArgs, reply *AssignReply) error {
	for _, task := range c.MapTasks.Tasks {
		task.State.mu.Lock()
		fmt.Println("task")
		fmt.Println(task)
		if task.State.State == IDLE {
			reply.TaskId = task.Id
			reply.Filename = task.Filename
			reply.TaskType = MAP_TASK
			reply.NReduce = c.NReduce
			fmt.Println("reply")
			fmt.Println(reply)
			task.State.State = IN_PROGRESS
			task.State.mu.Unlock()
			return nil
		}
		task.State.mu.Unlock()
	}
	return nil
}

func (c *Coordinator) AssignReduce(args *AssignArgs, reply *AssignReply) error {
	for _, task := range c.ReduceTasks.Tasks {
		task.State.mu.Lock()
		fmt.Println("task")
		fmt.Println(task)
		if task.State.State == IDLE {
			reply.TaskId = task.Id
			reply.Filenames = task.Filenames
			reply.TaskType = REDUCE_TASK
			reply.NReduce = c.NReduce
			fmt.Println("reply")
			fmt.Println(reply)
			task.State.State = IN_PROGRESS
			task.State.mu.Unlock()
			return nil
		}
		task.State.mu.Unlock()

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
	var err error
	fmt.Println("Complete")
	fmt.Println(args)
	if args.TaskType == MAP_TASK {
		err = c.HandleMapComplete(args, reply)
	} else if args.TaskType == REDUCE_TASK {
		err = c.HandleReduceComplete(args, reply)
	}
	if err != nil {
		return err
	}

	taskType := c.GetTaskType()
	if taskType == MAP_TASK && c.MapTasks.RemainCount.Value() == 0 {
		c.InitReduceTasks()
	}
	return nil
}

func (c *Coordinator) HandleMapComplete(args *CompleteArgs, reply *CompleteReply) error {
	for _, task := range c.MapTasks.Tasks {
		if task.Id == args.TaskId && task.State.GetState() != COMPLETED {
			task.State.SetState(COMPLETED)
			c.MapTasks.RemainCount.Dec()
		}
	}

	return nil
}

func (c *Coordinator) HandleReduceComplete(args *CompleteArgs, reply *CompleteReply) error {
	for _, task := range c.ReduceTasks.Tasks {
		if task.Id == args.TaskId && task.State.GetState() != COMPLETED {
			task.State.SetState(COMPLETED)
			c.ReduceTasks.RemainCount.Dec()
		}
	}
	return nil
}

func (c *Coordinator) Init(files []string) error {
	// caveat: task by filename without measure file size now.
	for i, filename := range files {
		task := &Task{Id: i, Filename: filename, State: &SafeState{}}
		c.MapTasks.Tasks = append(c.MapTasks.Tasks, task)
	}
	c.MapTasks.RemainCount = &SafeCounter{count: len(files)}
	c.ReduceTasks.RemainCount = &SafeCounter{count: c.NReduce}
	return nil
}

func (c *Coordinator) InitReduceTasks() {
	for i := 0; i < c.NReduce; i++ {
		task := &Task{Filenames: []string{}, State: &SafeState{}, Timer: 0}
		for _, task := range c.MapTasks.Tasks {
			filename := fmt.Sprintf("mr-%v-%v", task.Id, i)
			task.Filenames = append(task.Filenames, filename)
		}
		// note: may have a locality issue here
		c.ReduceTasks.Tasks = append(c.ReduceTasks.Tasks, task)
	}
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
	return ret
}

func (c *Coordinator) HandleTimeoutTasks() {
	for !c.Done() {
		time.Sleep(time.Second)
		taskType := c.GetTaskType()
		var tasks []*Task
		if taskType == MAP_TASK {
			tasks = c.MapTasks.Tasks
		} else {
			tasks = c.ReduceTasks.Tasks
		}
		for _, task := range tasks {
			task.Timer += 1
			if task.Timer >= TIMER_LIMIT && task.State.GetState() != COMPLETED {
				task.State.SetState(IDLE)
			}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := &MapTasks{}
	reduceTasks := &ReduceTasks{}
	c := Coordinator{NReduce: nReduce, MapTasks: mapTasks, ReduceTasks: reduceTasks}
	c.Init(files)
	c.server()
	// go c.HandleTimeoutTasks()
	return &c
}
