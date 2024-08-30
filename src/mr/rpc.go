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

// Add your RPC definitions here.

type GetnReduceArgs struct {
}

type GetnReduceReply struct {
	NReduce int
}

type AssignMapTaskArgs struct {
}

type AssignMapTaskReply struct {
	TaskId   int
	Filename string
}

type AllMapTasksDoneArgs struct {
}

type AllMapTasksDoneReply struct {
	Done bool
}

type NotifyOneMapTaskDoneArgs struct {
	TaskId       int
	Intermediate []string
}

type NotifyOneMapTaskDoneReply struct {
}

type ShutdownArgs struct {
}

type ShutdownReply struct {
}

type AssignReduceTaskArgs struct {
}

type AssignReduceTaskReply struct {
	TaskId int
	Files  []string
}

type NotifyOneReduceTaskDoneArgs struct {
	TaskId int
}

type NotifyOneReduceTaskDoneReply struct {
}

type AllReduceTasksDoneArgs struct {
}

type AllReduceTasksDoneReply struct {
	Done bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
