package shardctrler

import (
	"strconv"
	"strings"

	"6.5840/kvraft"
)

const (
	StrGroup = "Group"
	StrShard = "Shard"
	Sep      = "#"
)

var DummyConfig = Config{Groups: make(map[int][]string)}

type ConfigStateMachine interface {
	kvraft.StateMachine
	Join(map[int][]string) kvraft.Err
	Leave([]int) kvraft.Err
	Move(int, int) kvraft.Err
	Query(int) (Config, kvraft.Err)
}

type MemoryConfigStateMachine struct {
	Consistent *Consistent
	Configs    []Config
}

func NewMemoryConfigStateMachine() *MemoryConfigStateMachine {
	return &MemoryConfigStateMachine{
		Consistent: NewConsistent(),
		Configs:    []Config{DummyConfig},
	}
}

func (m *MemoryConfigStateMachine) Join(servers map[int][]string) kvraft.Err {
	lastCfg := m.Configs[len(m.Configs)-1]
	newCfg := Config{Num: len(m.Configs), Shards: lastCfg.Shards, Groups: deepCopy(lastCfg.Groups)}

	for k, v := range servers {
		if _, ok := newCfg.Groups[k]; !ok {
			newCfg.Groups[k] = v
			m.Consistent.Add(wrapGroup(k))
		}
	}

	for i := range newCfg.Shards {
		if s, err := m.Consistent.Get(wrapShard(i)); err == nil {
			newCfg.Shards[i] = unwrap(s)
		} else {
			newCfg.Shards[i] = 0
		}
	}

	m.Configs = append(m.Configs, newCfg)
	return kvraft.OK
}

func (m *MemoryConfigStateMachine) Leave(gids []int) kvraft.Err {
	lastCfg := m.Configs[len(m.Configs)-1]
	newCfg := Config{Num: len(m.Configs), Shards: lastCfg.Shards, Groups: deepCopy(lastCfg.Groups)}

	for _, v := range gids {
		if _, ok := newCfg.Groups[v]; ok {
			delete(newCfg.Groups, v)
			m.Consistent.Remove(wrapGroup(v))
		}
	}

	for i := range newCfg.Shards {
		if s, err := m.Consistent.Get(wrapShard(i)); err == nil {
			newCfg.Shards[i] = unwrap(s)
		} else {
			newCfg.Shards[i] = 0
		}
	}

	m.Configs = append(m.Configs, newCfg)
	return kvraft.OK
}

func (m *MemoryConfigStateMachine) Move(shard, gid int) kvraft.Err {
	lastCfg := m.Configs[len(m.Configs)-1]
	newCfg := Config{Num: len(m.Configs), Shards: lastCfg.Shards, Groups: deepCopy(lastCfg.Groups)}

	newCfg.Shards[shard] = gid

	m.Configs = append(m.Configs, newCfg)
	return kvraft.OK
}

func (m *MemoryConfigStateMachine) Query(num int) (Config, kvraft.Err) {
	if num == -1 || num >= len(m.Configs) {
		return m.Configs[len(m.Configs)-1], kvraft.OK
	} else if num >= 0 {
		return m.Configs[num], kvraft.OK
	} else {
		return m.Configs[0], ErrNoConfig
	}
}

func (m *MemoryConfigStateMachine) Apply(iop kvraft.IOp) (reply kvraft.Reply) {
	switch op := iop.(Op); op.Type {
	case OpJoin:
		reply.Err = m.Join(op.Servers)
	case OpLeave:
		reply.Err = m.Leave(op.GIDs)
	case OpMove:
		reply.Err = m.Move(op.Shard, op.GID)
	case OpQuery:
		reply.Value, reply.Err = m.Query(op.Num)
	}
	return
}

func deepCopy(original map[int][]string) map[int][]string {
	copied := make(map[int][]string, len(original))
	for k, v := range original {
		copied[k] = v
	}
	return copied
}

func wrapGroup(i int) string { return StrGroup + Sep + strconv.Itoa(i) }

func wrapShard(i int) string { return StrShard + Sep + strconv.Itoa(i) }

func unwrap(s string) int {
	ss := strings.Split(s, Sep)
	i, _ := strconv.Atoi(ss[len(ss)-1])
	return i
}
