package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type Request struct {
	WorkId int
	InputFileName string
	OutputFileName []string
}

type Reply struct {
	// case 1: map
	// case 2: reduce
	// case 3: wait
	// case 4: all_work_done
	WorkType string
	WorkId int
	InputFileName []string
	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
