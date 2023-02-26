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

type AssignArgs struct {
}

type AssignReply struct {
	Filename  string   // for map
	Filenames []string // for reduce
	TaskType  TASK_TYPE
	TaskId    int
	NReduce   int
}

type CompleteArgs struct {
	TaskType TASK_TYPE
	TaskId   int
}

type CompleteReply struct {
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
