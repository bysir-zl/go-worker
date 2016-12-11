package worker

import (
	"testing"
	"log"
)

func TestServer(t *testing.T) {
	w := NewServer("127.0.0.1:6379")
	w.Handle("order", func(j *job) JobStatus {
		orderId, ok := j.Param("id")
		if ok {
			log.Print("orderId:", orderId)
		} else {
			log.Print("order error")
		}
		return JobStatusRetryNow
	})
	w.Handle("auth", func(j *job) JobStatus {
		orderId, ok := j.Param("id")
		if ok {
			log.Print("authId:", orderId)
		} else {
			log.Print("auth error")
		}
		return JobStatusRetryWait
	})
	w.Handle("user", func(j *job) JobStatus {
		orderId, ok := j.Param("id")
		if ok {
			log.Print("userId:", orderId)
		} else {
			log.Print("user error")
		}
		return JobStatusSuccess
	})
	w.Listen(func(status int, j *job) {
		// todo
	})
	w.Server()
}

func TestClient(t *testing.T) {
	w := NewClient("127.0.0.1:6379")
	j := NewJob("order")
	j.SetParam("id", "10086")
	w.Push(j)
	j = NewJob("auth")
	j.SetParam("id", "1")
	w.Push(j)
	j = NewJob("user")
	j.SetParam("id", "2")
	w.Push(j)
}

// 54485 ns/op
func BenchmarkClient(b *testing.B) {
	w := NewClient("127.0.0.1:6379")
	j := NewJob("order")
	j.SetParam("id", "1")
	for i := 0; i < b.N; i++ {
		w.Push(j)
	}
}