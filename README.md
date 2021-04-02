#### MIT 6.824 LAB
- [x] Lab 1: MapReduce
- [ ] Lab 2: Raft
- [ ] Lab 3: KV Raft
- [ ] Lab 4: Sharded KV


#### Lab 1: MapReduce

Q1：`Lab1`比较简单，分别实现`Worker.go`和`Master.go`里的代码逻辑就行，主要讲一下超时机制，也就是在`crash`条件下，`worker`可能无法完成工作的情况？

>   解决思路是：在`Master`里起一个协程`manager_unfinish`，里面有一个类似于`UnMapWork`的`channel`, `Master`给`Worker`分配任务的时候顺便将任务`id`
同时`push`到这个`channel`中，`manager_unfinish`从`channel`收到任务`id`后就另外起协程`map_time_alert`，它们的工作是先计时`10s`，然后看对应的
`Worker`是否完成并返回，否则就讲此任务`id`再一次`push`到`MapWork`的`channel`中，继续分配任务。`Reduce`也是同理。

![mapreduce](https://user-images.githubusercontent.com/10417157/113435914-51f2f380-9416-11eb-9589-0fcccd22b051.png)

