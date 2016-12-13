package worker

import (
	"log"
	"math"
	"time"
)

type JobFlag int

const (
	JobFlagRetryWait JobFlag = iota // wait 2^count second then retry
	JobFlagRetryNow  // retry now
	JobFlagSuccess
	JobFlagFailed
)

const maxRetryCount byte = 5

type worker struct {
}

type workerServer struct {
	consumers       []Consumer              //
	listener        func(j *Job, err error) //
	consumerCreator func(topic, channel string) (Consumer, error)
}

type Handler func(j *Job) JobFlag

func (p *workerServer) callListen(j *Job, err error) {
	if p.listener != nil {
		p.listener(j, err)
	}
}

func (p *workerServer) Handle(topic, channel string, fun Handler) (err error) {
	consumer, err := p.consumerCreator(topic, channel)
	if err != nil {
		return
	}
	consumer.Handle(func(j *Job) {
		go p.doJob(j, fun)
	})
	p.consumers = append(p.consumers, consumer)
	return
}

func (p *workerServer) doJob(job *Job, fun Handler) {
	defer func() {
		i := recover()
		if i != nil {
			job.Status = JobStatusFailed
			e, ok := i.(error)
			if ok {
				p.callListen(job, e)
			} else {
				p.callListen(job, nil)
			}
		}
	}()

	flag := fun(job)
	switch flag {
	case JobFlagRetryNow:
		job.count++
		if job.count <= maxRetryCount {
			p.doJob(job, fun)
		} else {
			job.Status = JobStatusFailed
			p.callListen(job, nil)
		}
	case JobFlagRetryWait:
		job.count++
		if job.count <= maxRetryCount {
			t := int64(math.Pow(1.9, float64(job.count)))
			if t > 60 {
				t = 60
			}
			d := time.Duration(t * int64(time.Second))
			<-time.After(d)
			p.doJob(job, fun)
		} else {
			job.Status = JobStatusFailed
			p.callListen(job, nil)
		}
	case JobFlagSuccess:
		job.Status = JobStatusSuccess
		p.callListen(job, nil)
	case JobFlagFailed:
		job.Status = JobStatusFailed
		p.callListen(job, nil)
	}
}

func (p *workerServer) Server() {
	var exitChan chan int

	for _, c := range p.consumers {
		c.Server()
		go func(s chan int) {
			<-s
			exitChan <- 0
		}(c.StopChan())
	}

	<-exitChan
	close(exitChan)

	for _, c := range p.consumers {
		c.Stop()
	}

	log.Println("server exited")
}

func (p *workerServer) Listen(fun func(j *Job, err error)) {
	p.listener = fun
	return
}

func NewServer(consumerCreator func(topic, channel string) (Consumer, error)) (*workerServer, error) {
	c := workerServer{
		consumers: []Consumer{},
		consumerCreator:consumerCreator,
	}
	return &c, nil
}

type workerClient struct {
	producer Producer
}

func (p *workerClient) Push(jobs ...*Job) {
	p.producer.Publish(jobs...)
}

func NewClient(producerCreator func() (Producer, error)) (*workerClient, error) {
	p, err := producerCreator()
	if err != nil {
		return nil, err
	}
	c := workerClient{
		producer:p,
	}
	return &c, nil
}

