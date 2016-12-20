package worker

import (
	"log"
	"testing"
	"time"
)

const NsqdHost = "127.0.0.1:4150"

var wf = NewWorker(ConsumerForNsq(NsqdHost), ProducerForNsq(NsqdHost))

func TestPublish(t *testing.T) {
	c, _ := wf.Client()

	j := NewJob("order")
	j.SetParam("id", "1")
	c.Push(j)
	//c.Push(j)
}

func TestHandle(t *testing.T) {
	s, _ := wf.Server()
	s.Handle("order", "work", func(j *Job) (JobFlag, error) {
		log.Println("work - ", j)
		<-time.After(2 * time.Second)
		return FSuccess,nil
	})

	s.Handle("order", "loger", func(j *Job) (JobFlag, error) {
		log.Println("loger - ", j)
		<-time.After(1 * time.Second)
		return FRetryNow, nil
	})

	j := NewJob("Loopper")
	j.SetInterval(time.Second * 2)
	s.AddLoopJob(j, func(j *Job) (JobFlag, error) {
		log.Print("loop - ", j)
		return FSuccess, nil
	})

	s.Listen(func(j *Job, err error) {
		log.Println("listen - ", j, err)
	})

	s.Server()
}

// 98004 ns/op
func BenchmarkPublish(b *testing.B) {
	worker := NewWorker(ConsumerForNsq(NsqdHost), ProducerForNsq(NsqdHost))
	c, _ := worker.Client()

	j := NewJob("order")
	j.SetParam("id", "1")
	//j.SetParam("name", "bysir")
	for i := 0; i < b.N; i++ {
		c.Push(j)
	}
}
