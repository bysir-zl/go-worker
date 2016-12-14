package worker


type WorkerClient struct {
	producer Producer
}

func (p *WorkerClient) Push(jobs ...*Job)error {
	return p.producer.Publish(jobs...)
}

func NewClient(producerCreator func() (Producer, error)) (*WorkerClient, error) {
	p, err := producerCreator()
	if err != nil {
		return nil, err
	}
	c := WorkerClient{
		producer:p,
	}
	return &c, nil
}

func NewClientForNsq(host string) (*WorkerClient, error) {
	return NewClient(func() (Producer, error) {
		return NewNsqProducer(host)
	})
}
