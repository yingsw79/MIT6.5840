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
	"strings"
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
		log.Fatalln("os.Getwd", err)
	}

	getnReduceReply := &GetnReduceReply{}
	err = call("Coordinator.GetnReduce", &GetnReduceArgs{}, getnReduceReply)
	if err != nil {
		log.Fatalln("Coordinator.GetnReduce", err)
	}
	nReduce := getnReduceReply.NReduce

	var wg sync.WaitGroup

	// The map part
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				allMapTasksDoneReply := &AllMapTasksDoneReply{}
				err := call("Coordinator.AllMapTasksDone", &AllMapTasksDoneArgs{}, allMapTasksDoneReply)
				if err != nil || allMapTasksDoneReply.Done {
					log.Println("Map worker exit...")
					return
				}

				assignMapTaskReply := &AssignMapTaskReply{}
				err = call("Coordinator.AssignMapTask", &AssignMapTaskArgs{}, assignMapTaskReply)
				if err != nil {
					log.Println("Coordinator.AssignMapTask:", err)
					time.Sleep(time.Second)
					continue
				}

				go func() {
					fin, err := os.Open(assignMapTaskReply.Filename)
					if err != nil {
						log.Println("os.Open:", err)
						return
					}
					defer fin.Close()
					content, err := io.ReadAll(bufio.NewReader(fin))
					if err != nil {
						log.Println("io.ReadAll:", err)
						return
					}

					kvs := mapf(assignMapTaskReply.Filename, string(content))

					mp := make(map[int]*json.Encoder)
					for _, kv := range kvs {
						i := ihash(kv.Key) % nReduce
						enc, ok := mp[i]
						if !ok {
							fout, err := os.CreateTemp(wd, "tempfile-*")
							if err != nil {
								log.Println("os.CreateTemp:", err)
								return
							}
							defer func() {
								fout.Close()
								os.Rename(fout.Name(), fmt.Sprintf("mr-%d-%d", assignMapTaskReply.Id, i))
							}()
							enc = json.NewEncoder(fout)
							mp[i] = enc
						}

						if err := enc.Encode(&kv); err != nil {
							log.Println("Encode:", err)
							return
						}
					}

					err = call("Coordinator.NotifyOneMapTaskDone", &NotifyOneMapTaskDoneArgs{assignMapTaskReply.Id}, &NotifyOneMapTaskDoneReply{})
					if err != nil {
						log.Println("Coordinator.NotifyOneMapTaskDone:", err)
					}
				}()
			}
		}()
	}
	wg.Wait()

	// The reduce part

	for i := 0; i < nReduce; i++ {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			fout, err := os.CreateTemp(wd, "tempfile-*")
			if err != nil {
				log.Println("os.CreateTemp:", err)
				return
			}
			bw := bufio.NewWriter(fout)
			defer func() {
				bw.Flush()
				fout.Close()
				os.Rename(fout.Name(), fmt.Sprintf("mr-out-%d", n))
			}()

			files, err := os.ReadDir(wd)
			if err != nil {
				log.Println("os.ReadDir", err)
				return
			}

			mp := make(map[string][]string)
			for _, file := range files {
				if !file.IsDir() && strings.HasSuffix(file.Name(), fmt.Sprintf("-%d", n)) {
					fin, err := os.Open(filepath.Join(wd, file.Name()))
					if err != nil {
						log.Println("os.Open:", err)
						return
					}
					defer fin.Close()

					dec := json.NewDecoder(bufio.NewReader(fin))
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							if err != io.EOF {
								log.Println("Decode:", err)
								return
							} else {
								break
							}
						}
						k := kv.Key
						mp[k] = append(mp[k], kv.Value)
					}
				}
			}

			for k, v := range mp {
				if _, err := fmt.Fprintf(bw, "%s %s\n", k, reducef(k, v)); err != nil {
					log.Println("fmt.Fprintf:", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	err = call("Coordinator.Shutdown", &ShutdownArgs{}, &ShutdownReply{})
	if err != nil {
		log.Println("Coordinator.Shutdown:", err)
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
