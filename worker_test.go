package worker

import (
	"testing"
	"log"
	"time"
	"strconv"
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
	s.Handle("order", "work", func(j *Job) JobFlag {
		id, _ := j.Param("id")
		log.Println("work - ", "order id: " + id + " #" + strconv.Itoa(int(j.count)))
		<-time.After(2 * time.Second)
		return JobFlagSuccess
	})

	s.Handle("order", "loger", func(j *Job) JobFlag {
		id, _ := j.Param("id")
		log.Println("loger - ", "order id: " + id + " #" + strconv.Itoa(int(j.count)))
		<-time.After(1 * time.Second)
		return JobFlagRetryNow
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
