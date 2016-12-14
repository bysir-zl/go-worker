#go-worker 
worker 4 nsq

## usage

client.go
```go
const NsqdHost = "127.0.0.1:4150"
c, _ := NewClientForNsq(NsqdHost)

j := NewJob("order")
j.SetParam("id", "1")
c.Push(j)
```
service.go
```go
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
```

it will print :
```
2016/12/14 00:20:34 INF    1 [order/work] (127.0.0.1:4150) connecting to nsqd
2016/12/14 00:20:34 INF    2 [order/loger] (127.0.0.1:4150) connecting to nsqd
2016/12/14 00:20:40 work -  order id: 1 #1
2016/12/14 00:20:40 loger -  order id: 1 #1
2016/12/14 00:20:41 loger -  order id: 1 #2
2016/12/14 00:20:42 loger -  order id: 1 #3
2016/12/14 00:20:42 listen -  0 order,work:id=1#1 <nil>
2016/12/14 00:20:43 loger -  order id: 1 #4
2016/12/14 00:20:44 loger -  order id: 1 #5
2016/12/14 00:20:45 listen -  1 order,loger:id=1#5 <nil>
```
