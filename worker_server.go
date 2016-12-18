package worker

import (
	"errors"
	"fmt"
	"log"
	"math"
	"time"
)

// todo 需要根据实际情况调优
const maxGoRoutine int = 30000

type worker struct {
}

type WorkerServer struct {
	consumers       []Consumer              //
	listener        func(j *Job, err error) //
	consumerCreator func(topic, channel string) (Consumer, error)
	ch              chan int // channel buffer to controll max jobs
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
		go p.callJob(j, fun, p.ch)
	})
	p.consumers = append(p.consumers, consumer)
	return
}

func (p *WorkerServer) callJob(job *Job, handler Handler, finish chan int) {
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
			<-p.ch
		}
	}()

	job.Count++
	flag, err := handler(job)

	switch flag {
	case LRetryNow:
		if job.Count < job.MaxRetry {
			job.Status = SRetrying
			if job.Count == 1 {
				// notify is retrying
				p.callListen(job, err)
			}

			p.callJob(job, handler, nil)
		} else {
			job.Status = SFailed
			p.callListen(job, err)
		}
	case LRetryWait:
		if job.Count < job.MaxRetry {
			job.Status = SRetrying
			if job.Count == 1 {
				// notify is retrying
				p.callListen(job, err)
			}

			t := int64(math.Pow(1.8, float64(job.Count)))
			t = t + 2
			if t > 32 {
				t = 32
			}
			d := time.Duration(t * 1e9)
			<-time.After(d)
			p.callJob(job, handler, nil)
		} else {
			job.Status = SFailed
			p.callListen(job, err)
		}
	case LSuccess:
		job.Status = SSuccess
		p.callListen(job, err)
	case LFailed:
		job.Status = SFailed
		p.callListen(job, err)
	case LDelete:
		job.Status = SFinish
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

// add a looped job. will call listener with status is SFinish if it stopped
func (p *WorkerServer) AddLoopJob(j *Job, fun Handler) {
	loop := func(j *Job) {
		log.Printf("[WORKER] LoopJob %s is running\n", j.Topic())
		for {
			<-time.After(j.interval)
			p.callJob(j, fun, nil)
			if j.Status == SFinish {
				break
			}
		}
		log.Printf("[WORKER] LoopJob %s is stopped\n", j.Topic())
	}
	go loop(j)
}

func NewServer(consumerCreator func(topic, channel string) (Consumer, error)) (*WorkerServer, error) {
	c := WorkerServer{
		consumers:       []Consumer{},
		consumerCreator: consumerCreator,
		ch:              make(chan int, maxGoRoutine),
	}
	return &c, nil
}

func NewServerForNsq(host string) (*WorkerServer, error) {
	return NewServer(func(topic, channel string) (Consumer, error) {
		return NewNsqConsumer(host, topic, channel)
	})
}
