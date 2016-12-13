package worker

import (
	"bytes"
)

type JobStatus int

const (
	JobStatusSuccess JobStatus = iota
	JobStatusFailed // mark job failed
)

type Job struct {
	topic  []byte // topic name
	keys   [][]byte
	values [][]byte
	count  byte   // retry count
	Status JobStatus
}

func NewJob(topic string) *Job {
	return &Job{
		topic:s2B(topic),
	}
}

// encode to []byte like "key=value&name=bysir#0"
func (p *Job) encode() []byte {
	var queryBuf bytes.Buffer
	if p.keys != nil {
		for i, k := range p.keys {
			if i != 0 {
				queryBuf.WriteByte('&')
			}
			queryBuf.Write(k)
			queryBuf.WriteByte('=')
			queryBuf.Write(p.values[i])
		}
	}
	queryBuf.WriteByte('#')
	queryBuf.WriteByte(p.count)

	return queryBuf.Bytes()
}

func (p *Job) decode(topic, data []byte) bool {
	p.topic = topic

	vAndC := bytes.Split(data, []byte{'#'})
	values := vAndC[0]
	if len(vAndC) == 2 && len(vAndC[1]) != 0 {
		p.count = vAndC[1][0]
	}
	if len(values) != 0 {
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
	}
	return true
}

func (p *Job) String() string {
	var buf bytes.Buffer
	buf.Write(p.topic)
	buf.WriteByte(':')
	data := p.encode()
	dataLen := len(data)
	data = append(data[0:dataLen - 1], data[dataLen - 1] + 48)
	buf.Write(data)
	return buf.String()
}

func (p *Job) Param(key string) (value string, ok bool) {
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

func (p *Job) SetParam(key string, value string) {
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
