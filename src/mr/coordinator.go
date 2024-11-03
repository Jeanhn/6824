package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	"6.5840/mr/coordinate"
	"6.5840/mr/util"
)

type Coordinator struct {
	// Your definitions here.
	tm      *coordinate.TaskManager
	se      *coordinate.SplitExecutor
	nReduce int
	taskId  string
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

func (c *Coordinator) Acquire(args *AcquireArgs, reply *AcquireReply) error {
	task, err := c.tm.Acquire()
	if err != nil {
		return err
	}
	reply.Task = task
	reply.WorkerId = args.WorkerId

	log.Default().Printf("Coordinator.Acquire, args:%v, reply:%v\n", args, reply)

	return nil
}

func (c *Coordinator) Finish(args *FinishArgs, reply *FinishReply) error {
	err := c.tm.Finish(args.Task.Id)
	if err != nil {
		return err
	}
	log.Default().Printf("Coordinator.Finish, args:%v, reply:%v\n", args, reply)

	return nil
}

func (c *Coordinator) IsDone(args *IsDoneArgs, reply *IsDoneReply) error {
	done := c.tm.Done()

	reply.IsDone = done

	log.Default().Printf("Coordinator.IsDone, args:%v, reply:%v\n", args, reply)
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
	ret := c.tm.Done()

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.

	// split the input first
	defer util.FlushLogs()
	defer util.RemoveTempFiles()

	log.Default().Printf("MakeCoordinator start, files:%v, nReduce:%v\n", files, nReduce)

	taskId := util.RandomTaskId()
	se, err := coordinate.NewSplitExecutor(files, coordinate.SPLIT_SIZE, taskId)
	if err != nil {
		log.Default().Printf("MakeCoordinator error:%v\n", err)
		panic(err)
	}

	ok, err := se.Iterate()
	for ok && err == nil {
		ok, err = se.Iterate()
	}
	if err != nil {
		panic(err)
	}

	splitFiles := se.GetSplitFiles()

	tm, err := coordinate.NewTaskManager(splitFiles, taskId, 16, nReduce)
	if err != nil {
		panic(err)
	}

	c := Coordinator{
		taskId: taskId,
		se:     se,
		tm:     tm,
	}

	c.server()

	for !c.Done() {
		time.Sleep(time.Second)
	}
	return &c
}
