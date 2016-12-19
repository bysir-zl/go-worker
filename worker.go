package worker

type ConsumerCreator func(topic, channel string) (Consumer, error)
type ProducerCreator func() (Producer, error)

type Worker struct {
	consumerCreator ConsumerCreator
	producerCreator ProducerCreator
}

func NewWorker(consumerCreator ConsumerCreator, producerCreator ProducerCreator) *Worker {
	return &Worker{
		consumerCreator:consumerCreator,
		producerCreator:producerCreator,
	}
}

func (p *Worker) Client() (*Client, error) {
	return newClient(p)
}

func (p *Worker) Server() (*Server, error) {
	return newServer(p)
}


func ConsumerForNsq(host string) ConsumerCreator {
	return func(topic, channel string) (Consumer, error) {
		return NewNsqConsumer(host, topic, channel)
	}
}

func ProducerForNsq(host string) ProducerCreator {
	return func() (Producer, error) {
		return NewNsqProducer(host)
	}
}
