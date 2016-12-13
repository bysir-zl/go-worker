#go-worker 
worker 4 nsq

## usage
service.go
```go
const NsqdHost = "127.0.0.1:4150"
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

client.go
```go
c, _ := NewClientForNsq(NsqdHost)

j := NewJob("order")
j.SetParam("id", "1")
c.Push(j)
```