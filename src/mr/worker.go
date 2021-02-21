package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		request := &Request{}
		reply := &Reply{}
		// 如果没有请求到数据，直接退出了
		if !RequestForWork(request, reply, "Master.assign_work") {
			log.Fatal("ReuqestForWork fialed")
			break
		}
		switch reply.WorkType {
		case "map":
			do_map_work(mapf, reply)
		case "reduce":
			do_reduce_work(reducef, reply)
		case "wait":
			do_wait_work(reply)
		case "all_work_done":
			// 所有工作都做完了，直接退出
			break
		default:
			log.Fatal("Reply type error")
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func do_map_work(mapf func(string, string) []KeyValue,
				 reply *Reply) {
	// start map work
	// map任务一个InputFile
	filename := reply.InputFileName[0]
	workid := reply.WorkId
	NReduce := reply.NReduce
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannt open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("canot read %v", filename)
	}
	file.Close()
	kva := mapf(filename ,string(content))

	// open NReduce file
	encs := []*json.Encoder{}
	files := []*os.File{}
	for i := 0; i < NReduce; i++ {
		prefix := "mr-"
		// 以mr开头的临时文件, Rename后file不存在了
		ofile, _ := ioutil.TempFile("./map_temp", prefix)
		enc := json.NewEncoder(ofile)
		encs = append(encs, enc)
		files = append(files, ofile)
	}

	// write intermediate data
	for i := 0; i < len(kva); i++{
		index := ihash(kva[i].Key)
		err := encs[index].Encode(&kva[i])
		if err != nil {
			log.Fatal("Map func write intermediate error")
		}
	}

	// complete map work
	// first atomic rename the outfile
	reduce_id := 0
	out_file := []string{}
	for _, file := range files {
		newname := "./data/mr-" + strconv.Itoa(workid) + "-" + strconv.Itoa(reduce_id)
		os.Rename("./temp/" + file.Name(), newname)
		reduce_id++
		out_file = append(out_file, newname)
	}
	
	complete_request := &Request{}
	complete_request.OutputFileName = out_file
	complete_request.WorkId = workid

	complete_reply := &Reply{}
	if !RequestForWork(complete_request, complete_reply, "Master.complete_map") {
		log.Fatal("ReuqestForWork fialed")
	}

}

func do_reduce_work(reducef func(string, []string) string,
					reply *Reply) {
	// start reduce work
	// reduce任务NReduce个InputFile
	filename := reply.InputFileName
	workid := reply.WorkId

	intermediate := []KeyValue{}
	var kv KeyValue
	for _, name := range filename {
		file, err := os.Open(name)
		if err != nil {
			log.Fatalf("cannt open %v", name)
		}
		dec := json.NewDecoder(file)
		for {
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()	
	}
	// 按照key排序
	sort.Sort(ByKey(intermediate))

	prefix := "mr-out-"
	// 以mr开头的临时文件, Rename后file不存在了
	ofile, _ := ioutil.TempFile("./reduce_temp", prefix)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// complete reduce work
	// first atomic rename the outfile
	out_file := []string{}
	newname := "mr-out-" + strconv.Itoa(workid)
	os.Rename("./reduce_temp/" + ofile.Name(), newname)
	out_file = append(out_file, newname)
	
	complete_request := &Request{}
	complete_request.OutputFileName = out_file
	complete_request.WorkId = workid

	complete_reply := &Reply{}
	if !RequestForWork(complete_request, complete_reply, "Master.complete_reduce") {
		log.Fatal("ReuqestForWork fialed")
	}
}

func do_wait_work(reply *Reply) {
	// 等待1s, 然后自动进入了do_work 循环，再次请求数据
	time.Sleep(time.Second)
}

func RequestForWork(request *Request, reply *Reply, worktype string) bool {
	// request for work
	return call(worktype, &request, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
