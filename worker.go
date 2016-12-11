package worker

import (
	"github.com/deepzz0/go-com/log"
	"time"
	"math"
)

const maxRetryCount byte = 5

type worker struct {
	queue Queue
	paths map[string]func(j *job) JobStatus
}

type workerServer worker

type workerClient worker

func NewClient(redisHost string) *workerClient {
	c := workerClient(worker{
		queue:NewRedisQueue(redisHost),
		paths:map[string]func(j *job) JobStatus{},
	})
	return &c
}
func NewServer(redisHost string) *workerServer {
	c := workerServer(
		worker{
			queue:NewRedisQueue(redisHost),
			paths:map[string]func(j *job) JobStatus{},
		})
	return &c
}

func (p *workerServer) Handle(path string, fun func(j *job) JobStatus) {
	p.paths[path] = fun
}

func (p *workerServer) Listen(fun func(status int, j *job)) {
	// todo
	return
}
func (p *workerServer) Server() error {
	keys := []string{}
	for k := range p.paths {
		keys = append(keys, k)
	}
	var ch chan *job = make(chan *job, len(keys) * 10)
	go func() {
		for _, queue := range keys {
			// run go num is queue number
			go func(queue string) {
				for {
					job, err := p.queue.Pop(queue)
					if err != nil {
						log.Warn(err)
					} else {
						ch <- job
					}
				}
			}(queue)
		}
	}()

	for job := range ch {
		go p.do(job)
	}
	return nil
}

func (p *workerServer) do(j *job) {
	if fun, ok := p.paths[string(j.queue)]; ok {
		status := fun(j)
		switch status {
		case JobStatusSuccess:
		case JobStatusRetryWait:
			if j.count < maxRetryCount {
				j.count++
				t := int64(math.Pow(2, float64(j.count)))
				d := time.Duration(t * int64(time.Second))
				<-time.After(d)
				p.queue.Push(j)
			} else {
				log.Warn("job failed: ", j.String())
			}
		case JobStatusRetryNow:
			if j.count < maxRetryCount {
				j.count++
				p.queue.Push(j)
			} else {
				log.Warn("job failed: ", j.String())
			}
		default:
			log.Warn("handle function must return value in " +
				"[JobStatusRetryNow,JobStatusRetryWait,JobStatusSuccess]")
		}
	} else {
		log.Debug("not found url: " + string(j.queue))
	}
}

func (p *workerClient) Push(jobs ...*job) {
	p.queue.Push(jobs...)
}
