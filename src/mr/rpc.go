package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"

	"6.5840/mr/coordinate"
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

type AcquireArgs struct {
	workerId string
}

type AcquireReply struct {
	workerId string
	task     *coordinate.Task
}

type FinishArgs struct {
	workerId string
	task     coordinate.Task
}

type FinishReply struct {
}

type IsDoneArgs struct {
	workerId string
	taskId   string
}

type IsDoneReply struct {
	done bool
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
