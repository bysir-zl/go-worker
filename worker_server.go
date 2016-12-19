package worker

import (
	"errors"
	"fmt"
	"log"
	"time"
)

type JobFlag int

// todo 需要根据实际情况调优
// 这里还需要注意一个问题, 就是当一个job需要延时重试的时候, 将长时占用routine, 可能这是一个问题
// 但是又不能重新放回队列, 这样所有channel都会收到信息
const maxGoRoutine int = 30000

const (
	JobFlagRetryWait JobFlag = iota // wait 2+3*count second and retry
	JobFlagRetryNow                 // retry now
	JobFlagSuccess
	JobFlagFailed
)

const maxRetryCount int = 5

type Server struct {
	consumers []Consumer              //
	producer  Producer                //
	listener  func(j *Job, err error) //
	ch        chan int
	parent    *Factory
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
		go p.callJob(j, fun)
	})
	p.consumers = append(p.consumers, consumer)
	return
}

func (p *Server) callJob(job *Job, handler Handler) {

	job.addCount()

	defer func() {
		i := recover()
		if i != nil {
			job.Status = JobStatusFailed
			e, ok := i.(error)
			if ok {
				p.callListen(job, e)
			} else {
				p.callListen(job, errors.New(fmt.Sprint(i)))
			}
		}
		<-p.ch
	}()

	flag, err := handler(job)
	c := job.Count()
	switch flag {
	case JobFlagRetryNow:
		if c < maxRetryCount {
			job.Status = JobStatusRetrying
			if c == 1 {
				// notify is retrying
				p.callListen(job, err)
			}
			p.callJob(job, handler)
		} else {
			job.Status = JobStatusFailed
			p.callListen(job, err)
		}
	case JobFlagRetryWait:
		if c < maxRetryCount {
			job.Status = JobStatusRetrying
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
			p.callJob(job, handler)
		} else {
			job.Status = JobStatusFailed
			p.callListen(job, err)
		}
	case JobFlagSuccess:
		job.Status = JobStatusSuccess
		p.callListen(job, err)
	case JobFlagFailed:
		job.Status = JobStatusFailed
		p.callListen(job, err)
	}
}

func (p *Server) Server() (error) {
	var exitChan chan int

	for _, c := range p.consumers {
		err := c.Server()
		if err != nil {
			return err
		}
		go func(s chan int) {
			<-s
			exitChan <- 0
		}(c.StopChan())
	}

	log.Printf("[WORKER] started %d wroker(s)", len(p.consumers))
	<-exitChan
	log.Println("[WORKER] server exited")
	close(exitChan)

	for _, c := range p.consumers {
		c.Stop()
	}
	return nil
}

func (p *Server) Listen(fun func(j *Job, err error)) {
	p.listener = fun
	return
}

func newServer(parent *Factory) (*Server, error) {
	producer, err := parent.producerCreator()
	if err != nil {
		return nil, err
	}
	c := Server{
		consumers:      []Consumer{},
		producer:       producer,
		parent:         parent,
		ch:             make(chan int, maxGoRoutine),
	}
	return &c, nil
}
