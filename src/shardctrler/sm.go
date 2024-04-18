package shardctrler

import (
	"strconv"
	"strings"
)

const (
	StrGroup = "Group"
	StrShard = "Shard"
	Sep      = "#"
)

type ConfigStateMachine interface {
	Join(map[int][]string) Err
	Leave([]int) Err
	Move(int, int) Err
	Query(int) (*Config, Err)
}

type MemoryConfigStateMachine struct {
	Consistent *Consistent
	Configs    []Config
}

func NewMemoryConfigStateMachine() *MemoryConfigStateMachine {
	return &MemoryConfigStateMachine{
		Consistent: NewConsistent(),
		Configs:    []Config{{Groups: make(map[int][]string)}},
	}
}

func (m *MemoryConfigStateMachine) Join(servers map[int][]string) Err {
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
	return OK
}

func (m *MemoryConfigStateMachine) Leave(gids []int) Err {
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
	return OK
}

func (m *MemoryConfigStateMachine) Move(shard, gid int) Err {
	lastCfg := m.Configs[len(m.Configs)-1]
	newCfg := Config{Num: len(m.Configs), Shards: lastCfg.Shards, Groups: deepCopy(lastCfg.Groups)}

	newCfg.Shards[shard] = gid

	m.Configs = append(m.Configs, newCfg)
	return OK
}

func (m *MemoryConfigStateMachine) Query(num int) (*Config, Err) {
	if num == -1 || num >= len(m.Configs) {
		return &m.Configs[len(m.Configs)-1], OK
	} else if num >= 0 {
		return &m.Configs[num], OK
	} else {
		return nil, ErrNoConfig
	}
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
