package worker

import "bytes"

type job struct {
	queue  []byte // queue's name
	keys   [][]byte
	values [][]byte
}

func NewJob(queue string) *job {
	return &job{
		queue:s2B(queue),
	}
}

func (p *job) encode() []byte {
	var queryBuf bytes.Buffer
	for i, k := range p.keys {
		if i != 0 {
			queryBuf.WriteByte('&')
		}
		queryBuf.Write(k)
		queryBuf.WriteByte('=')
		queryBuf.Write(p.values[i])
	}
	return queryBuf.Bytes()
}

func (p *job) decode(queue []byte, values []byte) bool {
	p.queue = queue
	vs := bytes.Split(values, []byte{'&'})
	vsLen := len(vs)
	p.values = make([][]byte, vsLen)
	p.keys = make([][]byte, vsLen)
	for i, v := range vs {
		kv := bytes.Split(v, []byte{'='})
		if len(kv) != 2 {
			return false
		}

		p.keys[i] = kv[0]
		p.values[i] = kv[1]
	}
	return true
}

func (p *job) Get(key string) (value string, ok bool) {
	if p.keys == nil {
		return
	}
	kb := s2B(key)
	for i, k := range p.keys {
		if bytes.Equal(k, kb) {
			value = b2S(p.values[i])
			ok = true
			return
		}
	}
	return
}

func (p *job) Set(key string, value string) {
	kb := s2B(key)
	vb := s2B(value)
	if p.keys == nil {
		p.values = [][]byte{vb}
		p.keys = [][]byte{kb}
	} else {
		p.keys = append(p.keys, kb)
		p.values = append(p.values, vb)
	}
	return
}

