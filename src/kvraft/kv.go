package kvraft

type KVStateMachine interface {
	Get(string) (string, Err)
	Put(string, string) Err
	Append(string, string) Err
}

type MemoryKV map[string]string

func NewMemoryKV() MemoryKV { return MemoryKV{} }

func (m MemoryKV) Get(k string) (string, Err) {
	if v, ok := m[k]; ok {
		return v, OK
	}

	return "", ErrNoKey
}

func (m MemoryKV) Put(k, v string) Err {
	m[k] = v
	return OK
}

func (m MemoryKV) Append(k, v string) Err {
	m[k] += v
	return OK
}
