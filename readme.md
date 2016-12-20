#go-worker
worker 4 nsq

## usage

client.go
```go
const NsqdHost = "127.0.0.1:4150"
var wf = NewWorker(ConsumerForNsq(NsqdHost), ProducerForNsq(NsqdHost))

c, _ := wf.Client()

j := NewJob("order")
j.SetParam("id", "1")
c.Push(j)
```
service.go
```go
s, _ := wf.Server()

s.Handle("order", "work", func(j *Job) (JobFlag,error) {
    log.Println("work - ", j)
    <-time.After(2 * time.Second)
    return FSuccess,nil
})

s.Handle("order", "loger", func(j *Job) (JobFlag,error)  {
    log.Println("loger - ", j)
    <-time.After(1 * time.Second)
    return FRetryNow,nil
})

s.Listen(func(j *Job, err error) {
    log.Println("listen - ", j.Status, j.String(), err)
})

s.Server()
```

it will print :
```
2016/12/14 21:43:23 INF    1 [order/work] (127.0.0.1:4150) connecting to nsqd
2016/12/14 21:43:23 INF    2 [order/loger] (127.0.0.1:4150) connecting to nsqd
2016/12/14 21:43:23 [WORKER] started 2 worker(s)
2016/12/14 21:43:25 loger -  DOING order,loger:id=1#1
2016/12/14 21:43:25 work -  DOING order,work:id=1#1
2016/12/14 21:43:26 listen -  3 RETRYING order,loger:id=1#1 <nil>
2016/12/14 21:43:26 loger -  RETRYING order,loger:id=1#2
2016/12/14 21:43:27 loger -  RETRYING order,loger:id=1#3
2016/12/14 21:43:27 listen -  1 SUCCESS order,work:id=1#1 <nil>
2016/12/14 21:43:28 loger -  RETRYING order,loger:id=1#4
2016/12/14 21:43:29 loger -  RETRYING order,loger:id=1#5
2016/12/14 21:43:30 listen -  2 FAILED order,loger:id=1#5 <nil>
```
