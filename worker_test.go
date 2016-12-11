package worker

import (
	"testing"
	"log"
)

func TestServer(t *testing.T) {
	w := NewServer("127.0.0.1:6379")
	w.Handle("order", func(j job) int {
		orderId, ok := j.Get("id")
		if ok {
			log.Print("orderId:", orderId)
		} else {
			log.Print("order error")
		}
		return 0
	})
	w.Handle("auth", func(j job) int {
		orderId, ok := j.Get("id")
		if ok {
			log.Print("authId:", orderId)
		} else {
			log.Print("auth error")
		}
		return 0
	})
	w.Server()
}

func TestClient(t *testing.T) {
	w := NewClient("127.0.0.1:6379")
	j := NewJob("order")
	j.Set("id", "10086")
	w.Push(j)
	j = NewJob("auth")
	j.Set("id", "1")
	w.Push(j)
}

// 54485 ns/op
func BenchmarkClient(b *testing.B) {
	w := NewClient("127.0.0.1:6379")
	j := NewJob("order")
	j.Set("id", "1")
	for i := 0; i < b.N; i++ {
		w.Push(j)
	}
}