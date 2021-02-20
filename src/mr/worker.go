package mr

import "fmt"
import "hash/fnv"
import "log"
import "net/rpc"
import "os"
import "io/ioutil"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	request := &Request{}
	reply := &Reply{}
	if !RequestForWork(request, reply) {
		log.Fatal("ReuqestForWork Fialed")
		return
	}
	switch reply.Worker_Type {
	case "map":
		do_map_work(reply)
	case "reduce":
		do_reduce_work(mapf, reply)
	case "wait":
		do_wait_work(reply)
	case "all_work_done":
		do_finish_work(reply)
	default:
		log.Fatal("Reply Type Error")
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func do_map_work(mapf func(string, string) []KeyValue,
				 reply *Reply) {
	filename := reply.Input_File_Name
	workerid := reply.Worker_Id
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
	sort.Sort(ByKey(kva))
	
}

func do_reduce_work(reply *Reply) {

}

func do_wait_work(reply *Reply) {

}

func do_finish_work(reply *Reply) {

}

func RequestForWork(request *Request, reply *Reply) bool {
	// request for work
	return call("Master.GetWork", &request, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
