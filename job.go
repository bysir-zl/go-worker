package worker

import (
	"bytes"
	"net/url"
	"strconv"
)

type JobStatus int

const (
	JobStatusDoing    JobStatus = iota
	JobStatusSuccess
	JobStatusFailed    //
	JobStatusRetrying  //
)

type Job struct {
	topic   []byte // topic name
	channel []byte // channel name, on listener and counter use it
	keys    [][]byte
	values  [][]byte

	count  int
	Status JobStatus
}

func NewJob(topic string) *Job {
	return &Job{
		topic:s2B(topic),
	}
}

// encode to []byte like "key=value&name=bysir"
func (p *Job) encode() []byte {
	var queryBuf bytes.Buffer
	if p.keys != nil {
		for i, k := range p.keys {
			if queryBuf.Len() != 0 {
				queryBuf.WriteByte('&')
			}
			queryBuf.Write(k)
			queryBuf.WriteByte('=')
			queryBuf.Write(p.values[i])
		}
	}

	// now, count is not save to queue
	//queryBuf.WriteByte('#')
	//queryBuf.WriteByte(p.count)

	return queryBuf.Bytes()
}

func (p *Job) decode(data []byte) bool {
	// now, count is not save to queue
	//vAndC := bytes.Split(data, []byte{'#'})
	//values := vAndC[0]
	//if len(vAndC) == 2 && len(vAndC[1]) != 0 {
	//	p.count = vAndC[1][0]
	//}
	values := data
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
	switch p.Status {
	case JobStatusFailed:
		buf.WriteString("FAILED ")
	case JobStatusSuccess:
		buf.WriteString("SUCCESS ")
	case JobStatusDoing:
		buf.WriteString("DOING ")
	case JobStatusRetrying:
		buf.WriteString("RETRYING ")
	}
	buf.Write(p.topic)
	buf.WriteByte(',')
	buf.Write(p.channel)
	buf.WriteByte(':')
	buf.Write(p.encode())
	buf.WriteByte('#')
	buf.WriteString(strconv.Itoa(p.count))
	return buf.String()
}

func (p *Job) Channel() string {
	return b2S(p.channel)
}

func (p *Job) Topic() string {
	return b2S(p.topic)
}
func (p *Job) Count() int {
	return p.count
}

func (p *Job) setCount(count int) {
	p.count = count
}

func (p *Job) addCount() {
	p.count++
}

func (p *Job) Param(key string) (value string, ok bool) {
	if p.keys == nil {
		return
	}
	kb := s2B(key)
	key, _ = url.QueryUnescape(key)
	for i, k := range p.keys {
		if bytes.Equal(k, kb) {
			value = b2S(p.values[i])
			value, _ = url.QueryUnescape(value)
			ok = true
			return
		}
	}
	return
}

func (p *Job) SetParam(key string, value string) {
	key = url.QueryEscape(key)
	value = url.QueryEscape(value)
	kb := s2B(key)
	vb := s2B(value)
	p.SetParamByte(kb, vb)
	return
}

func (p *Job) SetParamByte(kb, vb []byte) {
	if p.keys == nil {
		p.values = [][]byte{}
		p.keys = [][]byte{}
	}

	for i, k := range p.keys {
		if bytes.Equal(k, kb) {
			p.values[i] = vb
			return
		}
	}

	p.keys = append(p.keys, kb)
	p.values = append(p.values, vb)

	return
}
