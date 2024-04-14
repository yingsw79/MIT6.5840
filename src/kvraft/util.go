package kvraft

import (
	"log"
	"sync"
)

const debug = false

func DPrintf(format string, v ...any) {
	if debug {
		log.Printf(format, v...)
	}
}

type OpContext struct {
	Seq   int
	Err   Err
	Value string
}

type LastOps map[int64]OpContext

type cache struct {
	mu      sync.RWMutex
	lastOps LastOps
}

func newCache() *cache { return &cache{lastOps: LastOps{}} }

func (c *cache) load(k int64) (OpContext, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	v, ok := c.lastOps[k]
	return v, ok
}

func (c *cache) store(k int64, v OpContext) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastOps[k] = v
}

func (c *cache) getLastOps() LastOps {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.lastOps
}

func (c *cache) setLastOps(lastOps LastOps) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastOps = lastOps
}

type notifier struct {
	mu        sync.Mutex
	notifyMap map[int]chan Reply
}

func newNotifier() *notifier { return &notifier{notifyMap: make(map[int]chan Reply)} }

func (n *notifier) register(index int) <-chan Reply {
	n.mu.Lock()
	defer n.mu.Unlock()

	ch := make(chan Reply)
	n.notifyMap[index] = ch
	return ch
}

func (n *notifier) unregister(index int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if ch, ok := n.notifyMap[index]; ok {
		close(ch)
		delete(n.notifyMap, index)
	}
}

func (n *notifier) notify(index int, reply Reply) {
	if ch, ok := n.notifyMap[index]; ok {
		ch <- reply
	}
}
