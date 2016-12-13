package worker

import (
	"github.com/nsqio/go-nsq"
	"kuaifa.com/kuaifa/kuaifa-api-service/config"
)

type NsqConsumer struct {
	consumer *nsq.Consumer
	stopChan chan int
	topic    string
	channel  string
}

func NewNsqConsumer(topic string, channel string) (*NsqConsumer, error) {
	cfg := nsq.NewConfig()
	p, err := nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		return nil, err
	}

	return &NsqConsumer{
		consumer:p,
		stopChan:p.StopChan,
		topic:topic,
		channel:channel,
	}, nil
}

type NsqHandler struct {
	handler  func(j *Job)
	consumer *NsqConsumer
}

func (h *NsqHandler) HandleMessage(message *nsq.Message) error {
	var job Job
	job.decode(s2B(h.consumer.topic), message.Body)
	h.handler(&job)
	return nil
}

func (p *NsqConsumer) StopChan() chan int {
	return p.stopChan
}
func (p *NsqConsumer) Stop() {
	p.consumer.Stop()
}

func (p *NsqConsumer) Handle(fun func(j *Job)) {
	h := NsqHandler{handler:fun, consumer:p}
	p.consumer.AddHandler(&h)
	return
}

func (p *NsqConsumer) Server() (err error) {
	err = p.consumer.ConnectToNSQD(config.Config.NsqdHost)
	return
}

