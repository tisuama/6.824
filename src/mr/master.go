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

var logger *log.Logger

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
func (m* Master) AssignWork(request *Request, reply *Reply) error {
	// case 1.1: map
	// case 1.2: reduce
	// case 1.3: wait
	// case 1.4: all_work_done
	switch m.state {
	case "MAP":
		m.AssignMapWork(request, reply)
	case "REDUCE":
		m.AssignReduceWork(request, reply)
	case "DONE":
		m.AssignFinishWork(request, reply)
	default:
		return errors.New("Master cannt work")
	}
	return nil
}

func (m* Master) AssignMapWork(request *Request, reply *Reply) error {
	// case 1: 没有Map任务了
	select {
	case id := <-m.MapWork:
		reply.WorkType = "map"
		reply.WorkId = id
		reply.InputFileName = append(reply.InputFileName, m.MapFile[id])
		reply.NReduce = m.NReduce
		m.UnMapWork <- id
		logger.Printf("AssignMapWork: WorkType %v, WorkId %v, InputFileName %v",  "map", id, m.MapFile[id])
	default:
		// case 3.1: Map任务完成
		if m.CompleteMapNumber == len(m.MapFile) {
			m.state =  "REDUCE"
			// 向reduce通道发送数据
			for i := 0; i < m.NReduce; i++ {
				m.ReduceWork <- i
			}
			logger.Printf("AssignMapWork: All Map Complete")
			m.AssignWork(request, reply)
		} else {
			// case 3.2: 等待状态
			logger.Printf("AssignMapWork: some map uncomplete, wait some time")
			m.AssignWaitWork(request, reply)
		}
	}
	return nil
}

func (m* Master) AssignReduceWork(request *Request, reply *Reply) error {
	select {
	case id := <-m.ReduceWork:
		reply.WorkType = "reduce"
		reply.WorkId = id
		reply.InputFileName = m.ReduceFile[id]// 不用append了
		reply.NReduce = m.NReduce
		m.UnReduceWork <- id
		logger.Printf("AssignMapWork: WorkType %v, WorkId %v, InputFileName %v",  "reduce", id, m.ReduceFile[id])
	default:
		// case 3.1: Reduce任务完成
		if m.CompleteReduceNumber == m.NReduce {
			m.state =  "DONE"
			m.DONE = true
			logger.Printf("AssignReduceWork: All Reduce Complete")
			m.AssignWork(request, reply)
		} else {
			// case 3.2: 等待状态
			logger.Printf("AssignReduceWork: some reduce uncomplete, wait some time")
			m.AssignWaitWork(request, reply)
		}
	}
	return nil
}

func (m* Master) AssignWaitWork(request *Request, reply *Reply) error {
	reply.WorkType = "wait"
	return nil
}


func (m* Master) AssignFinishWork(request *Request, reply *Reply) error {
	reply.WorkType = "all_work_done"
	return nil
}


// case2: CompleteMap
func (m* Master) CompleteMap(request *Request, reply *Reply) error{
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
	m.MapComplete[request.WorkId] = true
	m.CompleteMapNumber++
	logger.Printf("CompleteMap: CompleteMapNumber: %v, UnComplete: %v", 
			m.CompleteMapNumber, len(m.MapFile) - m.CompleteMapNumber)
	return nil
}
// case3: CompleteReduce
func (m* Master) CompleteReduce(request *Request, reply *Reply) error{
	// case 1: 判断这个map的任务是不是已经被其他worker完成了
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.ReduceComplete[request.WorkId] == true {
		return nil
	}
	// case 2: complete有效
	m.ReduceComplete[request.WorkId] = true
	m.CompleteReduceNumber++
	logger.Printf("CompleteReduce: CompleteReduceNumber: %v, UnComplete: %v", 
			m.CompleteReduceNumber, len(m.ReduceFile) - m.CompleteReduceNumber)	
	return nil
}

func map_time_alert(m *Master, id int) {
	time.Sleep(time.Second * 10)
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.MapComplete[id] == false {
		logger.Printf("map_time_alert: map work %v not done, reassign the work", id)
		m.MapWork <- id
	}
}

func reduce_time_alert(m *Master, id int) {
	time.Sleep(time.Second * 10)
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.ReduceComplete[id] == false {
		logger.Printf("reduce_time_alert: reduce work %v not done, reassign the work", id)
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
	logger.Printf("=== Master Start Listen ====")
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
		logger.Printf("=== Master Thread End ===")
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

	// 初始化log模块
	file, _ := os.Create("master_log.txt")
	logger = log.New(file, "", log.Ldate | log.Ltime)
	logger.Printf("=== Start Master Thread ===")

	// Unfinish
	m.UnMapWork = make(chan int, 10)
	m.UnReduceWork = make(chan int, 10)

	// map init
	m.MapComplete = make(map[int]bool)
	m.ReduceComplete = make(map[int]bool)

	// Your code here.
	// 初始化ReduceWork
	m.ReduceWork = make(chan int, 10)				  // 通道缓冲暂时为10
	m.ReduceFile = make([][]string, NReduce, NReduce) // 切片的大小和容量都是NReduce
	m.NReduce = NReduce

	// 初始化MapWork
	m.MapWork = make(chan int, 10)
	for index, filename := range files {
		m.MapWork <- index
		m.MapFile = append(m.MapFile, filename)
		logger.Printf("MakeMaster: Append %d %v", index, filename)
	}
	m.state = "MAP"

	// 起一个routine为Work计时
	go manager_unfinish(&m)

	m.server()
	return &m
}
