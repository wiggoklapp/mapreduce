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

// Arguments and reply from the RPC

type TaskRequest struct {
}

type TaskReply struct {
	MapTask    MapTask
	ReduceTask ReduceTask
	NReduce    int
	TaskType   bool
}

// TRUE == MapTask and FALSE == ReduceTask
type TaskFinishedRequest struct {
	TaskNumber int
	TaskType   bool
}

type TaskFinishedReply struct {
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
