package worker

import (
	"github.com/deepzz0/go-com/log"
)

type worker struct {
	queue Queue
	paths map[string]func(j job) int
}

type workerServer worker

type workerClient worker

func NewClient(redisHost string) *workerClient {
	c := workerClient(worker{
		queue:NewRedisQueue(redisHost),
		paths:map[string]func(j job) int{},
	})
	return &c
}
func NewServer(redisHost string) *workerServer {
	c := workerServer(
		worker{
			queue:NewRedisQueue(redisHost),
			paths:map[string]func(j job) int{},
		})
	return &c
}

func (p *workerServer) Handle(path string, fun func(j job) int) {
	p.paths[path] = fun
}

func (p *workerServer) Server() error {
	var ch chan job = make(chan job, 20)
	keys := []string{}
	for k := range p.paths {
		keys = append(keys, k)
	}

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
		go p.run(job)
	}
	return nil
}

func (p *workerServer) run(j job) {
	if fun, ok := p.paths[string(j.queue)]; ok {
		fun(j)
	} else {
		log.Debug("not found url: " + string(j.queue))
	}
}

func (p *workerClient) Push(jobs ...*job) {
	p.queue.Push(jobs...)
}
