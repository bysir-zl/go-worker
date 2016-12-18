package worker

import (
	"bytes"
	"time"
)

const DefalutMaxRetryCount = 5

type JobStatus int

const (
	SDoing    JobStatus = iota
	SSuccess
	SRetrying  //
	SFailed    //
	SFinish    //
)

type JobFlag int

const (
	LRetryWait JobFlag = iota // wait 2+1.8^count second and retry
	LRetryNow                 // retry now
	LSuccess
	LFailed
	LDelete     // delete job, not do never
)

type Job struct {
	topic   []byte // topic name
	channel []byte // channel name, only listen use it

	keys     [][]byte
	values   [][]byte
	MaxRetry byte

	Count    byte // retry count
	Status   JobStatus
	interval time.Duration
}

var maxRetryKey = []byte("__MaxRetry")

func NewJob(topic string) *Job {
	return &Job{
		topic:   s2B(topic),
		MaxRetry:DefalutMaxRetryCount,
		interval:time.Minute,
	}
}

// encode to []byte like "key=value&name=bysir"
func (p *Job) encode() []byte {
	var queryBuf bytes.Buffer

	for i, k := range p.keys {
		if queryBuf.Len() != 0 {
			queryBuf.WriteByte('&')
		}
		queryBuf.Write(k)
		queryBuf.WriteByte('=')
		queryBuf.Write(p.values[i])
	}

	// add self params
	if queryBuf.Len() != 0 {
		queryBuf.WriteByte('&')
	}
	queryBuf.Write(maxRetryKey)
	queryBuf.WriteByte('=')
	queryBuf.WriteByte(p.MaxRetry + 48)

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

			if bytes.Equal(kv[0], maxRetryKey) {
				p.MaxRetry = kv[1][0] - 48
			} else {
				p.keys[i] = kv[0]
				p.values[i] = kv[1]
			}
		}
	}

	return true
}

func (p *Job) String() string {
	var buf bytes.Buffer
	switch p.Status {
	case SFailed:
		buf.WriteString("FAILED ")
	case SSuccess:
		buf.WriteString("SUCCESS ")
	case SDoing:
		buf.WriteString("DOING ")
	case SRetrying:
		buf.WriteString("RETRYING ")
	case SFinish:
		buf.WriteString("FINISHED ")
	}
	buf.Write(p.topic)
	buf.WriteByte(',')
	buf.Write(p.channel)
	buf.WriteByte(':')
	buf.Write(p.encode())
	buf.WriteByte('#')
	buf.WriteByte(byte(p.Count) + 48)
	return buf.String()
}

func (p *Job) Channel() string {
	return b2S(p.channel)
}

func (p *Job) Topic() string {
	return b2S(p.topic)
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
	p.SetParamByte(kb, vb)
	return
}

func (p *Job) SetParamByte(key []byte, value []byte) {
	if p.keys == nil {
		p.values = [][]byte{value}
		p.keys = [][]byte{key}
	} else {
		p.keys = append(p.keys, key)
		p.values = append(p.values, value)
	}
	return
}

func (p *Job) SetInterval(duration time.Duration) {
	p.interval = duration
}
