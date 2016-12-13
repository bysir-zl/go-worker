package worker

import (
	"github.com/nsqio/go-nsq"
	"kuaifa.com/kuaifa/kuaifa-api-service/config"
	"time"
)

type NsqProducer struct {
	producer *nsq.Producer
}

func NewNsqProducer() (*NsqProducer, error) {
	cfg := nsq.NewConfig()
	p, err := nsq.NewProducer(config.Config.NsqdHost, cfg)
	if err != nil {
		return nil, err
	}
	return &NsqProducer{
		producer:p,
	}, nil
}

func (p *NsqProducer) Publish(jobs ...*Job) (err error) {
	//p.producer.DeferredPublish()
	topic := b2S(jobs[0].topic)
	if len(jobs) == 1 {
		err = p.producer.Publish(topic, jobs[0].encode())
		return
	} else {
		bodies := make([][]byte, len(jobs))
		for i, j := range jobs {
			bodies[i] = j.encode()
		}
		err = p.producer.MultiPublish(topic, bodies)
		return
	}
}

func (p *NsqProducer) DeferredPublish(t time.Duration, jobs ...*Job) (err error) {
	topic := b2S(jobs[0].topic)

	for _, j := range jobs {
		err = p.producer.DeferredPublish(topic, t, j.encode())
		if err != nil {
			return
		}
	}
	return
}
