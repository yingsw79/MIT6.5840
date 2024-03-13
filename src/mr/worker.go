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

type MapWorker struct {
	nReduce int
	wd      string
	f       func(string, string) []KeyValue
	tasks   chan *AssignMapTaskReply
	once    sync.Once
	done    chan struct{}
}

func (m *MapWorker) Request() {
	c, err := rpc.DialHTTP("unix", coordinatorSock())
	if err != nil {
		log.Fatalln("[MapWorker]:", err)
	}
	defer c.Close()

	m.once.Do(func() {
		go func() {
			for {
				allMapTasksDoneReply := &AllMapTasksDoneReply{}
				if err := c.Call("Coordinator.AllMapTasksDone", &AllMapTasksDoneArgs{}, allMapTasksDoneReply); err != nil ||
					allMapTasksDoneReply.Done {
					close(m.done)
					return
				}
				time.Sleep(time.Second)
			}
		}()
	})

	for {
		select {
		case <-m.done:
			return
		default:
		}

		assignMapTaskReply := &AssignMapTaskReply{}
		select {
		case <-m.done:
			return
		case call := <-c.Go("Coordinator.AssignMapTask", &AssignMapTaskArgs{}, assignMapTaskReply, make(chan *rpc.Call, 1)).Done:
			if call.Error != nil {
				time.Sleep(time.Second)
			} else {
				select {
				case <-m.done:
					return
				case m.tasks <- assignMapTaskReply:
				}
			}
		}
	}
}

func (m *MapWorker) Do() {
	for {
		select {
		case <-m.done:
			return
		default:
		}

		select {
		case <-m.done:
			return
		case task := <-m.tasks:
			m.do(task)
		}
	}
}

func (m *MapWorker) Done() <-chan struct{} {
	return m.done
}

func (m *MapWorker) do(task *AssignMapTaskReply) {
	if task == nil {
		return
	}

	defer func() {
		if err := recover(); err != nil {
			log.Println("[MapWorker]:", err)
		}
	}()

	log.Println("[MapWorker]: assigned to map task", task.TaskId)

	fin, err := os.Open(task.Filename)
	if err != nil {
		panic(err)
	}
	defer fin.Close()
	content, err := io.ReadAll(bufio.NewReader(fin))
	if err != nil {
		panic(err)
	}

	kvs := m.f(task.Filename, string(content))

	type info struct {
		enc    *json.Encoder
		name   string
		rename string
	}
	infos := make([]*info, m.nReduce)

	for _, kv := range kvs {
		i := ihash(kv.Key) % m.nReduce
		if infos[i] == nil {
			fout, err := os.CreateTemp(m.wd, "map-temp*")
			if err != nil {
				panic(err)
			}
			defer func() {
				fout.Close()
				os.Remove(fout.Name())
			}()

			infos[i] = &info{json.NewEncoder(fout), fout.Name(), filepath.Join(m.wd, fmt.Sprintf("mr-%d-%d", task.TaskId, i))}
		}

		if err := infos[i].enc.Encode(&kv); err != nil {
			panic(err)
		}
	}

	intermediate := make(map[int]string, m.nReduce)
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

	go func() {
		if err := call("Coordinator.NotifyOneMapTaskDone", &NotifyOneMapTaskDoneArgs{task.TaskId, intermediate}, &NotifyOneMapTaskDoneReply{}); err != nil {
			log.Println("[MapWorker]:", err)
		}
	}()
}

type ReduceWorker struct {
	nReduce int
	wd      string
	f       func(string, []string) string
	tasks   chan *AssignReduceTaskReply
	once    sync.Once
	done    chan struct{}
}

func (r *ReduceWorker) Request() {
	c, err := rpc.DialHTTP("unix", coordinatorSock())
	if err != nil {
		log.Fatalln("[ReduceWorker]:", err)
	}
	defer c.Close()

	for {
		allReduceTasksDoneReply := &AllReduceTasksDoneReply{}
		if err := call("Coordinator.AllReduceTasksDone", &AllReduceTasksDoneArgs{}, allReduceTasksDoneReply); err != nil ||
			allReduceTasksDoneReply.Done {
			return
		}

		assignReduceTaskReply := &AssignReduceTaskReply{}
		if err := call("Coordinator.AssignReduceTask", &AssignReduceTaskArgs{}, assignReduceTaskReply); err != nil {
			time.Sleep(time.Second)
		} else {
			r.tasks <- assignReduceTaskReply
		}
	}
}

func (r *ReduceWorker) Do() {
	for task := range r.tasks {
		r.do(task)
	}
}

func (r *ReduceWorker) Done() <-chan struct{} {
	return r.done
}

func (r *ReduceWorker) do(task *AssignReduceTaskReply) {
	if task == nil {
		return
	}

	defer func() {
		if err := recover(); err != nil {
			log.Println("[ReduceWorker]:", err)
		}
	}()

	log.Println("[ReduceWorker]: assigned to reduce task", task.TaskId)

	fout, err := os.CreateTemp(r.wd, "reduce-temp*")
	if err != nil {
		panic(err)
	}
	defer func() {
		fout.Close()
		os.Remove(fout.Name())
	}()
	bw := bufio.NewWriter(fout)

	mp := make(map[string][]string)
	for _, file := range task.Files {
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
		if _, err := fmt.Fprintf(bw, "%s %s\n", k, r.f(k, v)); err != nil {
			panic(err)
		}
	}

	if err := bw.Flush(); err != nil {
		panic(err)
	}

	if err := os.Rename(fout.Name(), fmt.Sprintf("mr-out-%d", task.TaskId)); err != nil {
		panic(err)
	}

	go func() {
		if err := call("Coordinator.NotifyOneReduceTaskDone", &NotifyOneReduceTaskDoneArgs{task.TaskId}, &NotifyOneReduceTaskDoneReply{}); err != nil {
			log.Println("[ReduceWorker]:", err)
		}
	}()
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

	// Map

	// Reduce

	if err := call("Coordinator.Shutdown", &ShutdownArgs{}, &ShutdownReply{}); err != nil {
		log.Println("[Worker]:", err)
	}
}

// send an RPC request to the coordinator, wait for the response.
func call(serviceMethod string, args any, reply any) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordinatorSock())
	if err != nil {
		return err
	}
	defer c.Close()

	return c.Call(serviceMethod, args, reply)
}
