package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Master struct {
	// Your definitions here.
	MapComplete map[int]bool		// map是否完成的映射
	ReduceComplete map[int]bool		// reduce是否完成的映射
	CompleteMapNumber int			// map完成数量
	CompleteReduceNumber int		// reduce完成数量
	ReduceWork chan int				// reduce任务 mr-X-Y
	MapWork chan int 				// map任务
	state string 					// Master的状态机 MAP/REDUCE/DONE
	lock sync.Mutex					// 并发
	NReduce int						// reduce数量
	MapFile []string				// map filename
	ReduceFile [][]string 			// reduce任务数

	// unfinish work
	UnMapWork chan int
	UnReduceWork chan int
	DONE bool						// work done
}

type MapTask struct{
	id int
	filename string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

// case1: GetWork 请求Map或者Reduce任务
func (m* Master) assign_work(request *Request, reply *Reply) error {
	// case 1.1: map
	// case 1.2: reduce
	// case 1.3: wait
	// case 1.4: all_work_done
	switch m.state {
	case "MAP":
		m.assign_map_work(request, reply)
	case "REDUCE":
		m.assign_reduce_work(request, reply)
	case "DONE":
		m.assign_finish_work(request, reply)
	default:
		return errors.New("Master cannt work")
	}
	return nil
}

func (m* Master) assign_map_work(request *Request, reply *Reply) {
	// case 1: 没有Map任务了
	select {
	case id := <-m.MapWork:
		reply.WorkType = "map"
		reply.WorkId = id
		reply.InputFileName = append(reply.InputFileName, m.MapFile[id])
		reply.NReduce = m.NReduce
		m.UnMapWork <- id
	default:
		// case 3.1: Map任务完成
		if m.CompleteMapNumber == len(m.MapFile) {
			m.state =  "REDUCE"
			// 向reduce通道发送数据
			for i := 0; i < m.NReduce; i++ {
				m.ReduceWork <- i
			}
			m.assign_work(request, reply)
		} else {
			// case 3.2: 等待状态
			m.assign_wait_work(request, reply)
		}
	}
}

func (m* Master) assign_reduce_work(request *Request, reply *Reply) {
	select {
	case id := <-m.ReduceWork:
		reply.WorkType = "reduce"
		reply.WorkId = id
		reply.InputFileName = m.ReduceFile[id]// 不用append了
		reply.NReduce = m.NReduce
		m.UnReduceWork <- id
	default:
		// case 3.1: Reduce任务完成
		if m.CompleteReduceNumber == m.NReduce {
			m.state =  "DONE"
			m.DONE = true
			m.assign_work(request, reply)
		} else {
			// case 3.2: 等待状态
			m.assign_wait_work(request, reply)
		}
	}
}

func (m* Master) assign_wait_work(request *Request, reply *Reply) {
	reply.WorkType = "wait"
}


func (m* Master) assign_finish_work(request *Request, reply *Reply) {
	reply.WorkType = "all_work_done"
}


// case2: CompleteMap
func (m* Master) complete_map(request *Request, reply *Reply) error{
	// Append map out file
	// case 1: 判断这个map的任务是不是已经被其他worker完成了
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.MapComplete[request.WorkId] == true {
		return nil
	}
	// case 2: complete有效
	for index, filename := range request.OutputFileName {
		m.ReduceFile[index] = append(m.ReduceFile[index], filename)
	}
	m.MapComplete[reply.WorkId] = true
	m.CompleteMapNumber++
	return nil
}
// case3: CompleteReduce
func (m* Master) complete_reduce(request *Request, reply *Reply) error{
	// case 1: 判断这个map的任务是不是已经被其他worker完成了
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.ReduceComplete[request.WorkId] == true {
		return nil
	}
	// case 2: complete有效
	m.ReduceComplete[reply.WorkId] = true
	m.CompleteReduceNumber++
	
	return nil
}

func map_time_alert(m *Master, id int) {
	time.Sleep(time.Second * 10)
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.MapComplete[id] == false {
		m.MapWork <- id
	}
}

func reduce_time_alert(m *Master, id int) {
	time.Sleep(time.Second * 10)
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.ReduceComplete[id] == false {
		m.ReduceWork <- id
	}
}

func manager_unfinish(m *Master) {
	for {
		select {
		// unfiished mapwork
		case t := <-m.UnMapWork:
			go map_time_alert(m, t)
		// unfinish reducework
		case id := <-m.UnReduceWork:
			go reduce_time_alert(m, id)
		}
	}
}

// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.DONE == true {
		ret = true
	}
	
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, NReduce int) *Master {
	m := Master{}

	// Your code here.
	// 初始化ReduceWork
	m.ReduceWork = make(chan int, 10)				  // 通道缓冲暂时为10
	m.ReduceFile = make([][]string, NReduce, NReduce) // 切片的大小和容量都是NReduce
	m.NReduce = NReduce

	// 初始化MapWork
	m.MapWork = make(chan int, 10)
	for index, filename := range os.Args[2:] {
		m.MapWork <- index
		m.MapFile = append(m.MapFile, filename)
	}
	m.state = "MAP"

	// 起一个routine为Work计时
	go manager_unfinish(&m)

	m.server()
	return &m
}
