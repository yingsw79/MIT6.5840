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

	// Map
	mLock    sync.Mutex
	mTasks   []string
	mPending map[int]chan struct{}
	mSeq     int

	nMap     int
	nMapDone atomic.Int32

	// Reduce
	rLock    sync.Mutex
	rTasks   map[int][]string
	rPending map[int]chan struct{}

	nReduce     int
	nReduceDone atomic.Int32

	shutdown atomic.Bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetnReduce(_ *GetnReduceArgs, reply *GetnReduceReply) error {
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) AssignMapTask(_ *AssignMapTaskArgs, reply *AssignMapTaskReply) error {
	c.mLock.Lock()
	defer c.mLock.Unlock()
	if len(c.mTasks) == 0 {
		return errors.New("there are currently no map tasks to assign")
	}

	id := c.mSeq
	c.mSeq++
	filename := c.mTasks[len(c.mTasks)-1]
	c.mTasks = c.mTasks[:len(c.mTasks)-1]
	done := make(chan struct{})
	c.mPending[id] = done
	reply.TaskId = id
	reply.Filename = filename

	go func() {
		select {
		case <-done:
			log.Printf("[Coordinator]: map task %d: done", id)
			c.nMapDone.Add(1)
		case <-time.After(10 * time.Second):
			log.Printf("[Coordinator]: map task %d: timeout", id)
			c.mLock.Lock()
			defer c.mLock.Unlock()

			close(done)
			delete(c.mPending, id)
			c.mTasks = append(c.mTasks, filename)
		}
	}()

	return nil
}

func (c *Coordinator) NotifyOneMapTaskDone(args *NotifyOneMapTaskDoneArgs, _ *NotifyOneMapTaskDoneReply) error {
	c.mLock.Lock()
	defer c.mLock.Unlock()

	ch, ok := c.mPending[args.TaskId]
	if !ok {
		return fmt.Errorf("map task %d is not pending", args.TaskId)
	}

	close(ch)
	delete(c.mPending, args.TaskId)

	c.rLock.Lock()
	defer c.rLock.Unlock()
	for i, v := range args.Intermediate {
		c.rTasks[i] = append(c.rTasks[i], v)
	}

	return nil
}

func (c *Coordinator) AllMapTasksDone(_ *AllMapTasksDoneArgs, reply *AllMapTasksDoneReply) error {
	reply.Done = int(c.nMapDone.Load()) == c.nMap
	return nil
}

func (c *Coordinator) Shutdown(_ *ShutdownArgs, _ *ShutdownReply) error {
	c.shutdown.Store(true)
	return nil
}

func (c *Coordinator) AssignReduceTask(_ *AssignReduceTaskArgs, reply *AssignReduceTaskReply) error {
	c.rLock.Lock()
	defer c.rLock.Unlock()

	if len(c.rTasks) == 0 {
		return errors.New("there are currently no reduce tasks to assign")
	}

	var id int
	var files []string
	for i, v := range c.rTasks {
		id = i
		files = v
		break
	}
	delete(c.rTasks, id)

	done := make(chan struct{})
	c.rPending[id] = done
	reply.TaskId = id
	reply.Files = files

	go func() {
		select {
		case <-done:
			log.Printf("[Coordinator]: reduce task %d: done", id)
			c.nReduceDone.Add(1)
		case <-time.After(10 * time.Second):
			log.Printf("[Coordinator]: reduce task %d: timeout", id)
			c.rLock.Lock()
			defer c.rLock.Unlock()

			close(done)
			delete(c.rPending, id)
			c.rTasks[id] = files
		}
	}()

	return nil
}

func (c *Coordinator) NotifyOneReduceTaskDone(args *NotifyOneReduceTaskDoneArgs, _ *NotifyOneReduceTaskDoneReply) error {
	c.rLock.Lock()
	defer c.rLock.Unlock()

	ch, ok := c.rPending[args.TaskId]
	if !ok {
		return fmt.Errorf("reduce task %d is not pending", args.TaskId)
	}

	close(ch)
	delete(c.rPending, args.TaskId)

	return nil
}

func (c *Coordinator) AllReduceTasksDone(_ *AllReduceTasksDoneArgs, reply *AllReduceTasksDoneReply) error {
	reply.Done = int(c.nMapDone.Load()) == c.nMap && int(c.nReduceDone.Load()) == c.nReduce
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
	c := &Coordinator{mTasks: files, mPending: make(map[int]chan struct{}), nMap: len(files),
		nReduce: nReduce, rTasks: make(map[int][]string), rPending: make(map[int]chan struct{})}
	c.server()
	log.Println("[Coordinator]: started")
	return c
}
