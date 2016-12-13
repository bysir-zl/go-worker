package worker

import (
	"testing"
	"github.com/nsqio/go-nsq"
	"kuaifa.com/kuaifa/kuaifa-api-service/config"
	"log"
)

type Hand1 struct{}
type Hand2 struct{}

func (p *Hand1)HandleMessage(message *nsq.Message) error {
	log.Printf("handler1 : %s", string(message.Body))
	return nil
}
func (p *Hand2)HandleMessage(message *nsq.Message) error {
	log.Printf("handler2 : %s", string(message.Body))
	return nil
}

func TestNsqPublish(t *testing.T) {
	cfg := nsq.NewConfig()
	p, err := nsq.NewProducer(config.Config.NsqdHost, cfg)
	if err != nil {
		log.Println(err)
		return
	}
	p.Publish("order", []byte("test"))
}

// publish one message, all Consumer will get it if they topic is same
func TestNsqHandle(t *testing.T) {
	cfg := nsq.NewConfig()
	p, err := nsq.NewConsumer("order", "default", cfg)
	if err != nil {
		log.Println(err)
		return
	}
	p.AddHandler(&Hand1{})
	p.ConnectToNSQD(config.Config.NsqdHost)

	p2, err := nsq.NewConsumer("order", "number", cfg)
	if err != nil {
		log.Println(err)
		return
	}
	p2.AddHandler(&Hand2{})
	p2.ConnectToNSQD(config.Config.NsqdHost)

	<-p.StopChan
}
//
//func TestClient(t *testing.T) {
//	p, err := NewNsqProducer()
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	for {
//		err = p.Publish("order", []byte("order"))
//		<-time.After(3*time.Second)
//	}
//	err = p.Publish("auth", []byte("auth"))
//	log.Println("push", err)
//}
//
//func BenchmarkProducer(b *testing.B) {
//	p, err := NewNsqProducer()
//	if err != nil {
//		return
//	}
//	for i := 0; i < b.N; i++ {
//		err = p.Publish("order", []byte("order"))
//	}
//}
//
//func TestService(t *testing.T) {
//	c, err := NewNsqConsumer("order")
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	c.Handle(func(msg *nsq.Message) error {
//		log.Println(string(msg.Body))
//		return nil
//	})
//
//	err = c.Server()
//	if err != nil {
//		t.Error(err)
//		return
//	}
//
//	for {
//		select {
//		case <-c.stopChan:
//			return
//		}
//	}
//
//	log.Println("stoped")
//}