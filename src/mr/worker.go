package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"

	"6.5840/mr/coordinate"
	"6.5840/mr/util"
	"6.5840/mr/work"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	defer util.FlushLogs()
	defer util.RemoveTempFiles()

	mapFunction := func(filename string, content string) []work.KeyValue {
		kvs := mapf(filename, content)
		ret := make([]work.KeyValue, 0)
		for _, kv := range kvs {
			v := work.KeyValue{
				Key:   kv.Key,
				Value: kv.Value,
			}
			ret = append(ret, v)
		}
		return ret
	}

	workId := util.RandomTaskId()
	for {
		task, err := Acquire(workId)
		if err != nil {
			panic(err)
		}
		if task == nil {
			continue
		}

		if task.Type == coordinate.MAP_TASK_TYPE {
			work.MapHandler(*task, mapFunction)
		} else if task.Type == coordinate.REDUCE_TASK_TYPE {
			work.ReduceHandler(*task, reducef)
		} else {
			panic(fmt.Sprintf("invalid type %v", task))
		}

		err = Finish(workId, *task)
		if err != nil {
			panic(err)
		}

		isDone, err := IsDone(workId, task.ProjectId)
		if isDone {
			break
		}

		time.Sleep(time.Millisecond * 200)
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	err := call("Coordinator.Example", &args, &reply)
	if err == nil {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return nil
	}

	fmt.Println(err)
	return err
}

func Acquire(workerId string) (*coordinate.Task, error) {
	request := AcquireArgs{
		WorkerId: workerId,
	}
	response := AcquireReply{}

	err := call("Coordinator.Acquire", &request, &response)
	if err != nil {
		return nil, err
	}
	return response.Task, nil
}

func Finish(workerId string, task coordinate.Task) error {
	request := FinishArgs{
		WorkerId: workerId,
		Task:     task,
	}
	response := FinishReply{}
	err := call("Coordinator.Finish", &request, &response)
	if err != nil {
		return err
	}

	return nil
}

func IsDone(workerId, taskId string) (bool, error) {
	request := IsDoneArgs{
		WorkerId: workerId,
		TaskId:   taskId,
	}
	response := IsDoneReply{}
	err := call("Coordinator.IsDone", &request, &response)
	if err != nil {
		return true, err
	}
	return response.IsDone, nil
}
