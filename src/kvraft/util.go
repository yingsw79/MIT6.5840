package kvraft

import (
	"sync"
	"time"
)

type OpContext struct {
	Seq int
	Reply
}

type LastOps map[int64]OpContext

type Cache struct {
	mu      sync.RWMutex
	lastOps LastOps
}

func NewCache() *Cache { return &Cache{lastOps: LastOps{}} }

func (c *Cache) Load(k int64) (OpContext, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	v, ok := c.lastOps[k]
	return v, ok
}

func (c *Cache) Store(k int64, v OpContext) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastOps[k] = v
}

func (c *Cache) LastOps() LastOps {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.lastOps
}

func (c *Cache) SetLastOps(lastOps LastOps) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastOps = lastOps
}

type Notifier struct {
	mu        sync.Mutex
	notifyMap map[int]chan Reply
}

func NewNotifier() *Notifier { return &Notifier{notifyMap: make(map[int]chan Reply)} }

func (n *Notifier) Register(index int) <-chan Reply {
	n.mu.Lock()
	defer n.mu.Unlock()

	ch := make(chan Reply)
	n.notifyMap[index] = ch
	return ch
}

func (n *Notifier) Unregister(index int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.notifyMap, index)
}

func (n *Notifier) Notify(index int, reply Reply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if ch, ok := n.notifyMap[index]; ok {
		go func() {
			select {
			case ch <- reply:
			case <-time.After(timeout):
			}
		}()
	}
}
