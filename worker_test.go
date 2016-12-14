package worker

import (
	"testing"
	"log"
	"time"
)

const NsqdHost = "127.0.0.1:4150"

func TestPublish(t *testing.T) {
	c, _ := NewClientForNsq(NsqdHost)

	j := NewJob("order")
	j.SetParam("id", "1")
	c.Push(j)
	//c.Push(j)
}

func TestHandle(t *testing.T) {
	s, _ := NewServerForNsq(NsqdHost)
	s.Handle("order", "work", func(j *Job) (JobFlag,error) {
		log.Println("work - ", j)
		<-time.After(2 * time.Second)
		return JobFlagSuccess,nil
	})

	s.Handle("order", "loger", func(j *Job) (JobFlag,error)  {
		log.Println("loger - ", j)
		<-time.After(1 * time.Second)
		return JobFlagRetryNow,nil
	})

	s.Listen(func(j *Job, err error) {
		log.Println("listen - ", j.Status, j.String(), err)
	})

	s.Server()
}

// 98004 ns/op
func BenchmarkPublish(b *testing.B) {
	c, _ := NewClientForNsq(NsqdHost)

	j := NewJob("order")
	j.SetParam("id", "1")
	//j.SetParam("name", "bysir")
	for i := 0; i < b.N; i++ {
		c.Push(j)
	}
}
