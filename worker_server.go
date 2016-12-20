package worker

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// todo 需要根据实际情况调优
// 这里还需要注意一个问题, 就是当一个job需要延时重试的时候, 将长时占用routine, 可能这是一个问题
// 但是又不能重新放回队列, 这样所有channel都会收到信息
const maxGoRoutine int = 30000

type Server struct {
	consumers []Consumer              //
	listener  func(j *Job, err error) //
	loopJobs  []LoopJob
	parent    *Factory
	ch        chan int // channel buffer to controll max jobs
	exitWG    sync.WaitGroup
}

type LoopJob struct {
	job    *Job
	handle Handler
}

type Handler func(j *Job) (JobFlag, error)

func (p *Server) callListen(j *Job, err error) {
	if p.listener != nil {
		defer func() {
			i := recover()
			if i != nil {
				log.Println("[WORKER] ", i)
			}
		}()

		p.listener(j, err)
	}

}

func (p *Server) Handle(topic, channel string, fun Handler) (err error) {
	consumer, err := p.parent.consumerCreator(topic, channel)
	if err != nil {
		return
	}
	consumer.Handle(func(j *Job) {
		// 限制并发协程数量
		p.ch <- 0
		go p.callJob(j, fun, p.ch)
	})
	p.consumers = append(p.consumers, consumer)
	return
}

func (p *Server) callJob(job *Job, handler Handler, finish chan int) {
	job.addCount()

	defer func() {
		i := recover()
		if i != nil {
			job.Status = SFailed
			e, ok := i.(error)
			if ok {
				p.callListen(job, e)
			} else {
				p.callListen(job, errors.New(fmt.Sprint(i)))
			}
		}
		if finish != nil {
			<-finish
		}
	}()

	flag, err := handler(job)
	c := job.Count()
	switch flag {
	case FRetryNow:
		if c < job.MaxRetry {
			job.Status = SRetrying
			if c == 1 {
				// notify is retrying
				p.callListen(job, err)
			}

			p.callJob(job, handler, nil)
		} else {
			job.Status = SFailed
			p.callListen(job, err)
		}
	case FRetryWait:
		if c < job.MaxRetry {
			job.Status = SRetrying
			if c == 1 {
				// notify is retrying
				p.callListen(job, err)
			}

			t := c * 2
			t = t + 2
			if t > 10 {
				t = 10
			}
			d := time.Duration(t * 1e9)
			<-time.After(d)
			p.callJob(job, handler, nil)
		} else {
			job.Status = SFailed
			p.callListen(job, err)
		}
	case FSuccess:
		job.Status = SSuccess
		p.callListen(job, err)
	case FFailed:
		job.Status = SFailed
		p.callListen(job, err)
	case FDelete:
		job.Status = SFinish
		p.callListen(job, err)
	}
}

func (p *Server) Server() (error) {
	if p.consumers != nil && len(p.consumers) != 0 {
		for _, c := range p.consumers {
			p.exitWG.Add(1)
			err := c.Server()
			if err != nil {
				return err
			}
			go func(c Consumer) {
				<-c.StopChan()
				p.exitWG.Done()
				c.Stop()
			}(c)
		}

		log.Printf("[WORKER] started %d wroker(s)", len(p.consumers))
	}

	if p.loopJobs != nil && len(p.loopJobs) != 0 {
		for _, l := range p.loopJobs {
			p.exitWG.Add(1)
			loop := func(l LoopJob) {
				log.Printf("[WORKER] LoopJob %s is running\n", l.job.Topic())
				for {
					var stop chan int
					go func(stop chan int) {
						l.job.Status = SDoing
						p.callJob(l.job, l.handle, nil)
						if l.job.Status == SFinish {
							stop <- 0
						}
					}(stop)

					select {
					case <-stop:
						break
					default:
						<-time.After(l.job.interval)
					}
				}
				log.Printf("[WORKER] LoopJob %s is stopped\n", l.job.Topic())
				p.exitWG.Done()
			}

			go loop(l)
		}
	}

	return nil
}

func (p *Server) Wait() {
	p.exitWG.Wait()
}

func (p *Server) Listen(fun func(j *Job, err error)) {
	p.listener = fun
	return
}

func newServer(parent *Factory) (*Server, error) {
	c := Server{
		consumers:      []Consumer{},
		parent:         parent,
		ch:             make(chan int, maxGoRoutine),
	}
	return &c, nil
}

// add a looped job. will call listener with status is SFinish if it stopped
func (p *Server) AddLoopJob(j *Job, fun Handler) {
	if p.loopJobs == nil {
		p.loopJobs = []LoopJob{{j, fun}}
	} else {
		p.loopJobs = append(p.loopJobs, LoopJob{j, fun})
	}
}
