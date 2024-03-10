package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu      sync.Mutex
	tasks   []string
	seq     int
	pending map[int]chan struct{}

	nTask   int
	nReduce int

	nDone    atomic.Int32
	shutdown atomic.Bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetnReduce(_ *GetnReduceArgs, reply *GetnReduceReply) error {
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) AssignMapTask(_ *AssignMapTaskArgs, reply *AssignMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.tasks) == 0 {
		return errors.New("there are currently no tasks to assign")
	}

	id := c.seq
	c.seq++
	filename := c.tasks[len(c.tasks)-1]
	c.tasks = c.tasks[:len(c.tasks)-1]
	done := make(chan struct{})
	c.pending[id] = done
	reply.Id = id
	reply.Filename = filename

	go func() {
		select {
		case <-done:
			c.nDone.Add(1)
		case <-time.After(10 * time.Second):
			c.mu.Lock()
			defer c.mu.Unlock()

			close(done)
			delete(c.pending, id)
			c.tasks = append(c.tasks, filename)
		}
	}()

	return nil
}

func (c *Coordinator) NotifyOneMapTaskDone(args *NotifyOneMapTaskDoneArgs, _ *NotifyOneMapTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch, ok := c.pending[args.Id]
	if !ok {
		return fmt.Errorf("task id %d does not exist", args.Id)
	}

	close(ch)
	delete(c.pending, args.Id)

	return nil
}

func (c *Coordinator) AllMapTasksDone(_ *AllMapTasksDoneArgs, reply *AllMapTasksDoneReply) error {
	reply.Done = int(c.nDone.Load()) == c.nTask
	return nil
}

func (c *Coordinator) Shutdown(_ *ShutdownArgs, _ *ShutdownReply) error {
	c.shutdown.Store(true)
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.

	return c.shutdown.Load()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := &Coordinator{tasks: files, pending: make(map[int]chan struct{}), nTask: len(files), nReduce: nReduce}
	c.server()
	return c
}
