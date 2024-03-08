package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			c, err := rpc.DialHTTP("unix", coordinatorSock())
			if err != nil {
				log.Println("dialing:", err)
				return
			}
			defer c.Close()

			for {
				allMapTasksDoneReply := &AllMapTasksDoneReply{}
				err = c.Call("Coordinator.AllMapTasksDone", &AllMapTasksDoneArgs{}, allMapTasksDoneReply)
				if err != nil || allMapTasksDoneReply.IsDone {
					return
				}

				assignMapTaskReply := &AssignMapTaskReply{}
				err = c.Call("Coordinator.AssignMapTask", &AssignMapTaskArgs{}, assignMapTaskReply)
				if err != nil {
					log.Println("Coordinator.AssignMapTask:", err)
					time.Sleep(time.Second)
					continue
				}

				f, err := os.Open(assignMapTaskReply.Filename)
				if err != nil {
					log.Println("os.Open:", err)
					continue
				}
				content, err := io.ReadAll(bufio.NewReader(f))
				if err != nil {
					log.Println("io.ReadAll:", err)
					continue
				}
				f.Close()

				kvs := mapf(assignMapTaskReply.Filename, string(content))
				
				fmap := map[int]io.Writer{}
				dir, _ := os.Getwd()
				f, err = os.CreateTemp(dir, "tempfile-*.json")
				if err != nil {
					log.Println("os.CreateTemp:", err)
					continue
				}
				enc := json.NewEncoder(f)
				for _, kv := range kvs {

					enc.Encode(&kv)
				}
				os.Rename(f.Name(), fmt.Sprintf("mr-%v%v", assignMapTaskReply.Id, ihash()%assignMapTaskReply.NReduce))

				err = c.Call("Coordinator.NotifyOneMapTaskDone", &NotifyOneMapTaskDoneArgs{assignMapTaskReply.Id}, &NotifyOneMapTaskDoneReply{})
				if err != nil {
					log.Println("Coordinator.NotifyOneMapTaskDone:", err)
				}
			}
		}()
	}
	wg.Wait()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
