package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "log"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id		int64
	seq 	int64
	leader  int 
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.seq = 1
	ck.id = nrand()
	ck.leader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	request := GetArgs{}
	request.Key = key
	request.Op = "Get"
	request.ClientId = ck.id
	request.Seq = ck.seq
	ck.seq++

	
	// block until reply back
	for true {
		reply := Reply{}
		log.Printf("client %v new Get request to index %v, request: %v", ck.id, ck.leader, request)
		ok := ck.servers[ck.leader].Call("KVServer.Get", &request, &reply)
		if ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			} else {
				ck.leader = (ck.leader + 1) % len(ck.servers)							
			}
		} else {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
	}
	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	request := PutAppendArgs{}
	request.Key = key
	request.Value = value
	request.Op = op
	request.ClientId = ck.id
	request.Seq = ck.seq
	ck.seq++

	for true {
		reply := Reply{}
		log.Printf("client %v new Put request to index %v, request: %v", ck.id, ck.leader, request)
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &request, &reply)
		if ok {
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader:
				ck.leader = (ck.leader + 1) % len(ck.servers)
				break
			default:
				break
			}
		} else { 
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
