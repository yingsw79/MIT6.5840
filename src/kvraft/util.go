package kvraft

import (
	"sync"
	"time"
)

type Notifier struct {
	mu        sync.Mutex
	notifyMap map[int]chan *Reply
}

func NewNotifier() *Notifier { return &Notifier{notifyMap: make(map[int]chan *Reply)} }

func (n *Notifier) Register(index int) <-chan *Reply {
	n.mu.Lock()
	defer n.mu.Unlock()

	ch := make(chan *Reply)
	n.notifyMap[index] = ch
	return ch
}

func (n *Notifier) Unregister(index int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.notifyMap, index)
}

func (n *Notifier) Notify(index int, reply *Reply) {
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
