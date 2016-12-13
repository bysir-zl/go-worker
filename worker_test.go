package worker

import (
	"testing"
	"log"
	"time"
)

func TestPublish(t *testing.T) {
	c, _ := NewClient()

	j := NewJob("order")
	j.SetParam("id", "1")
	j2 := NewJob("order")
	j2.SetParam("id", "2")
	c.Push(j)
	c.Push(j2)
	//c.Push(j)
	//c.Push(j2)
}

func TestHandle(t *testing.T) {
	s, _ := NewServer()
	s.Handle("order", "work", func(j *Job) JobFlag {
		id, _ := j.Param("id")
		log.Println("work - ", "order id: " + id)
		<-time.After(2 * time.Second)
		return JobFlagSuccess
	})

	s.Handle("order", "loger", func(j *Job) JobFlag {
		id, _ := j.Param("id")
		log.Println("loger - ", "order id: " + id)
		<-time.After(1 * time.Second)
		return JobFlagRetryNow
	})

	s.Listen(func(j *Job, err error) {
		log.Println("listen - ", j.Status, j.String())
	})

	s.Server()
}

// 98004 ns/op
func BenchmarkPublish(b *testing.B) {
	c, _ := NewClient()

	j := NewJob("order")
	j.SetParam("id", "1")
	//j.SetParam("name", "bysir")
	for i := 0; i < b.N; i++ {
		c.Push(j)
	}
}