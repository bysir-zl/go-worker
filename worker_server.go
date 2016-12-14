package worker

import (
	"log"
	"math"
	"time"
	"fmt"
	"errors"
)

type JobFlag int

// todo 需要根据实际情况调优
const maxGoRoutine int = 30000

const (
	JobFlagRetryWait JobFlag = iota // wait 2+1.8^count second and retry
	JobFlagRetryNow  // retry now
	JobFlagSuccess
	JobFlagFailed
)

const maxRetryCount int = 5

type worker struct {
}

type WorkerServer struct {
	consumers       []Consumer              //
	listener        func(j *Job, err error) //
	consumerCreator func(topic, channel string) (Consumer, error)
	ch              chan int
}

type Handler func(j *Job) (JobFlag, error)

func (p *WorkerServer) callListen(j *Job, err error) {
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

func (p *WorkerServer) Handle(topic, channel string, fun Handler) (err error) {
	consumer, err := p.consumerCreator(topic, channel)
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

func (p *WorkerServer) callJob(job *Job, handler Handler) {
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

	job.count++
	flag, err := handler(job)

	switch flag {
	case JobFlagRetryNow:
		if job.count < maxRetryCount {
			job.Status = JobStatusRetrying
			if job.count == 1 {
				// notify is retrying
				p.callListen(job, err)
			}

			p.callJob(job, handler)
		} else {
			job.Status = JobStatusFailed
			p.callListen(job, err)
		}
	case JobFlagRetryWait:
		if job.count < maxRetryCount {
			job.Status = JobStatusRetrying
			if job.count == 1 {
				// notify is retrying
				p.callListen(job, err)
			}

			t := int64(math.Pow(1.8, float64(job.count)))
			t = t + 2
			if t > 60 {
				t = 60
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

func (p *WorkerServer) Server() {
	var exitChan chan int

	for _, c := range p.consumers {
		c.Server()
		go func(s chan int) {
			<-s
			exitChan <- 0
		}(c.StopChan())
	}

	log.Printf("[WORKER] started %d worker(s)", len(p.consumers))
	<-exitChan
	log.Println("[WORKER] server exited")
	close(exitChan)

	for _, c := range p.consumers {
		c.Stop()
	}

}

func (p *WorkerServer) Listen(fun func(j *Job, err error)) {
	p.listener = fun
	return
}

func NewServer(consumerCreator func(topic, channel string) (Consumer, error)) (*WorkerServer, error) {
	c := WorkerServer{
		consumers: []Consumer{},
		consumerCreator:consumerCreator,
		ch:make(chan int,maxGoRoutine),
	}
	return &c, nil
}

func NewServerForNsq(host string) (*WorkerServer, error) {
	return NewServer(func(topic, channel string) (Consumer, error) {
		return NewNsqConsumer(host, topic, channel)
	})
}
