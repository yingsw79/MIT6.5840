package shardctrler

import (
	"slices"

	"6.5840/kvraft"
)

var DummyConfig = Config{Groups: make(map[int][]string)}

type ConfigStateMachine interface {
	kvraft.StateMachine
	Join(map[int][]string) kvraft.Err
	Leave([]int) kvraft.Err
	Move(int, int) kvraft.Err
	Query(int) (Config, kvraft.Err)
}

type MemoryConfigStateMachine struct{ Configs []Config }

func NewMemoryConfigStateMachine() *MemoryConfigStateMachine {
	return &MemoryConfigStateMachine{Configs: []Config{DummyConfig}}
}

func (m *MemoryConfigStateMachine) Join(servers map[int][]string) kvraft.Err {
	lastCfg := m.Configs[len(m.Configs)-1]
	newCfg := Config{Num: len(m.Configs), Shards: lastCfg.Shards, Groups: deepCopy(lastCfg.Groups)}

	for k, v := range servers {
		newCfg.Groups[k] = v
	}
	g := g2s(newCfg.Shards[:], newCfg.Groups)
	allocShards := []int{}
	if v, ok := g[0]; ok {
		allocShards = append(allocShards, v...)
		delete(g, 0)
	}

	newCfg.Shards = balance(g, allocShards)
	m.Configs = append(m.Configs, newCfg)
	return kvraft.OK
}

func (m *MemoryConfigStateMachine) Leave(gids []int) kvraft.Err {
	lastCfg := m.Configs[len(m.Configs)-1]
	newCfg := Config{Num: len(m.Configs), Shards: lastCfg.Shards, Groups: deepCopy(lastCfg.Groups)}

	g := g2s(newCfg.Shards[:], newCfg.Groups)
	allocShards := []int{}
	for _, v := range gids {
		delete(newCfg.Groups, v)
		if s, ok := g[v]; ok {
			allocShards = append(allocShards, s...)
			delete(g, v)
		}
	}
	if v, ok := g[0]; ok {
		allocShards = append(allocShards, v...)
		delete(g, 0)
	}

	newCfg.Shards = balance(g, allocShards)
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
		return DummyConfig, ErrNoConfig
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

func g2s(shards []int, groups map[int][]string) map[int][]int {
	mp := make(map[int][]int, len(groups))
	for k := range groups {
		mp[k] = []int{}
	}
	for i, v := range shards {
		mp[v] = append(mp[v], i)
	}
	return mp
}

func balance(g map[int][]int, allocShards []int) (res [NShards]int) {
	n := len(g)
	if n < 1 {
		return
	}

	x, y := NShards/n, NShards%n
	z := x
	if y > 0 {
		z++
	}

	groups := []int{}
	for k := range g {
		groups = append(groups, k)
	}
	slices.Sort(groups) // deterministic
	slices.SortFunc(groups, func(a, b int) int { return len(g[b]) - len(g[a]) })

	for i, v := range groups {
		t := x
		if i < y {
			t = z
		}
		if s := g[v]; len(s) > t {
			g[v] = s[:t]
			allocShards = append(allocShards, s[t:]...)
		}
	}

	for i, v := range groups {
		t := x
		if i < y {
			t = z
		}
		if n := len(g[v]); n < t {
			g[v] = append(g[v], allocShards[:t-n]...)
			allocShards = allocShards[t-n:]
		}
	}

	for k, shards := range g {
		for _, v := range shards {
			res[v] = k
		}
	}
	return
}
