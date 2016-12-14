package worker

import (
	"testing"
	"github.com/nsqio/go-nsq"
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
	p, err := nsq.NewProducer(NsqdHost, cfg)
	if err != nil {
		log.Println(err)
		return
	}
	p.Publish("order", []byte("test"))
}

// publish one message, all Consumer will get it if they's topic is same
func TestNsqHandle(t *testing.T) {
	cfg := nsq.NewConfig()
	p, err := nsq.NewConsumer("order", "default", cfg)
	if err != nil {
		log.Println(err)
		return
	}
	p.AddHandler(&Hand1{})
	p.ConnectToNSQD(NsqdHost)

	p2, err := nsq.NewConsumer("order", "number", cfg)
	if err != nil {
		log.Println(err)
		return
	}
	p2.AddHandler(&Hand2{})
	p2.ConnectToNSQD(NsqdHost)

	<-p.StopChan
}
