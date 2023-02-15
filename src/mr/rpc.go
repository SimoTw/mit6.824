package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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
type AssignArgs struct {
	WorkerId int
}

type CompleteArgs struct {
	Filename string
	TaskType TASK_TYPE
	WorkerId int
}

type CompleteReply struct{}

type AssignReply struct {
	Filename  string   // for map
	Filenames []string // for reduce
	TaskType  TASK_TYPE
	NReduce   int
	WorkerId  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
