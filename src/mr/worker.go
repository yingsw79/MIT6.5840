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
	"path/filepath"
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

	wd, err := os.Getwd()
	if err != nil {
		log.Fatalln("[Worker]:", err)
	}

	getnReduceReply := &GetnReduceReply{}
	err = call("Coordinator.GetnReduce", &GetnReduceArgs{}, getnReduceReply)
	if err != nil {
		log.Fatalln("[Worker]:", err)
	}
	nReduce := getnReduceReply.NReduce

	var wg sync.WaitGroup

	// Map
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerId int) {
			log.Printf("[Map worker %d]: started\n", workerId)
			defer wg.Done()

			for {
				allMapTasksDoneReply := &AllMapTasksDoneReply{}
				if err := call("Coordinator.AllMapTasksDone", &AllMapTasksDoneArgs{}, allMapTasksDoneReply); err != nil ||
					allMapTasksDoneReply.Done {
					log.Printf("[Map worker %d]: exit\n", workerId)
					return
				}

				assignMapTaskReply := &AssignMapTaskReply{}
				if err := call("Coordinator.AssignMapTask", &AssignMapTaskArgs{}, assignMapTaskReply); err != nil {
					log.Printf("[Map worker %d]: %s\n", workerId, err)
					time.Sleep(time.Second)
					continue
				}
				log.Printf("[Map worker %d]: assigned to map task %d\n", workerId, assignMapTaskReply.TaskId)

				go func() {
					defer func() {
						if err := recover(); err != nil {
							log.Printf("[Map worker %d]: %s\n", workerId, err)
						}
					}()

					fin, err := os.Open(assignMapTaskReply.Filename)
					if err != nil {
						panic(err)
					}
					defer fin.Close()
					content, err := io.ReadAll(bufio.NewReader(fin))
					if err != nil {
						panic(err)
					}

					kvs := mapf(assignMapTaskReply.Filename, string(content))

					type info struct {
						enc    *json.Encoder
						name   string
						rename string
					}
					infos := make([]*info, nReduce)

					for _, kv := range kvs {
						i := ihash(kv.Key) % nReduce
						if infos[i] == nil {
							fout, err := os.CreateTemp(wd, "map-temp*")
							if err != nil {
								panic(err)
							}
							defer func() {
								fout.Close()
								os.Remove(fout.Name())
							}()

							infos[i] = &info{json.NewEncoder(fout), fout.Name(), filepath.Join(wd, fmt.Sprintf("mr-%d-%d", assignMapTaskReply.TaskId, i))}
						}

						if err := infos[i].enc.Encode(&kv); err != nil {
							panic(err)
						}
					}

					intermediate := make(map[int]string, nReduce)
					for i, v := range infos {
						if v == nil {
							continue
						}
						if err := os.Rename(v.name, v.rename); err != nil {
							panic(err)
						} else {
							intermediate[i] = v.rename
						}
					}

					if err := call("Coordinator.NotifyOneMapTaskDone", &NotifyOneMapTaskDoneArgs{assignMapTaskReply.TaskId, intermediate}, &NotifyOneMapTaskDoneReply{}); err != nil {
						panic(err)
					}
				}()
			}
		}(i)
	}
	wg.Wait()

	// Reduce
	for i := 0; i < 5; i++ {
		wg.Add(1)

		go func(workerId int) {
			log.Printf("[Reduce worker %d]: started\n", workerId)
			defer wg.Done()

			for {
				allReduceTasksDoneReply := &AllReduceTasksDoneReply{}
				if err := call("Coordinator.AllReduceTasksDone", &AllReduceTasksDoneArgs{}, allReduceTasksDoneReply); err != nil ||
					allReduceTasksDoneReply.Done {
					log.Printf("[Reduce worker %d]: exit\n", workerId)
					return
				}

				assignReduceTaskReply := &AssignReduceTaskReply{}
				if err := call("Coordinator.AssignReduceTask", &AssignReduceTaskArgs{}, assignReduceTaskReply); err != nil {
					log.Printf("[Reduce worker %d]: %s\n", workerId, err)
					time.Sleep(time.Second)
					continue
				}
				log.Printf("[Reduce worker %d]: assigned to reduce task %d\n", workerId, assignReduceTaskReply.TaskId)

				go func() {
					defer func() {
						if err := recover(); err != nil {
							log.Printf("[Reduce worker %d]: %s\n", workerId, err)
						}
					}()

					fout, err := os.CreateTemp(wd, "reduce-temp*")
					if err != nil {
						panic(err)
					}
					defer func() {
						fout.Close()
						os.Remove(fout.Name())
					}()
					bw := bufio.NewWriter(fout)

					mp := make(map[string][]string)
					for _, file := range assignReduceTaskReply.Files {
						fin, err := os.Open(file)
						if err != nil {
							panic(err)
						}
						defer fin.Close()

						dec := json.NewDecoder(bufio.NewReader(fin))
						for {
							var kv KeyValue
							if err := dec.Decode(&kv); err != nil {
								if err != io.EOF {
									panic(err)
								} else {
									break
								}
							}
							k := kv.Key
							mp[k] = append(mp[k], kv.Value)
						}
					}

					for k, v := range mp {
						if _, err := fmt.Fprintf(bw, "%s %s\n", k, reducef(k, v)); err != nil {
							panic(err)
						}
					}

					if err := bw.Flush(); err != nil {
						panic(err)
					}

					if err := os.Rename(fout.Name(), fmt.Sprintf("mr-out-%d", assignReduceTaskReply.TaskId)); err != nil {
						panic(err)
					}

					if err := call("Coordinator.NotifyOneReduceTaskDone", &NotifyOneReduceTaskDoneArgs{assignReduceTaskReply.TaskId}, &NotifyOneReduceTaskDoneReply{}); err != nil {
						panic(err)
					}
				}()
			}
		}(i)
	}
	wg.Wait()

	if err := call("Coordinator.Shutdown", &ShutdownArgs{}, &ShutdownReply{}); err != nil {
		log.Println("[Worker]:", err)
	}
}

// send an RPC request to the coordinator, wait for the response.
func call(serviceMethod string, args any, reply any) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordinatorSock())
	if err != nil {
		log.Println("dialing:", err)
		return err
	}
	defer c.Close()

	return c.Call(serviceMethod, args, reply)
}
