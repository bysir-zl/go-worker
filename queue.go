package worker

import (
	"github.com/garyburd/redigo/redis"
	"errors"
)

const nameSpace = "goworker::"

type Queue interface {
	Push(values ...*job) (err error)
	Pop(queue string) (job *job, err error)
}

type redisQueue struct {
	redisPool *redis.Pool
}

// u can push multi job once, but they must in one queue
func (p *redisQueue) Push(jobs ...*job) (err error) {
	arg := make([]interface{}, len(jobs) + 1)
	arg[0] = nameSpace + b2S(jobs[0].queue)

	for i, j := range jobs {
		d := j.encode()
		arg[i + 1] = d
	}
	c := p.redisPool.Get()
	_, err = c.Do("LPUSH", arg...)
	c.Close()
	if err != nil {
		return
	}

	return
}

// it's block, maybe u need use 'go'
func (p *redisQueue) Pop(queue string) (j *job, err error) {
	c := p.redisPool.Get()
	v, err := c.Do("BRPOP", nameSpace + queue, "0")
	c.Close()
	if err != nil {
		return
	}
	vs, ok := v.([]interface{})
	if !ok || len(vs) != 2 {
		err = errors.New("redis return error")
		return
	}

	valueBs, ok2 := vs[1].([]byte)
	if !ok2 {
		err = errors.New("is not []byte")
		return
	}

	j = &job{}
	ok = j.decode(s2B(queue), valueBs)
	if !ok {
		err = errors.New("can not decode")
		return
	}

	ok = true
	return
}

func NewRedisQueue(redisHost string) *redisQueue {
	var pool = &redis.Pool{
		MaxIdle:   80,
		MaxActive: 12000, // max number of connections
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisHost)
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}

	r := redisQueue{
		redisPool: pool,
	}

	return &r
}

