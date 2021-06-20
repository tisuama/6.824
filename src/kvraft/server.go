 package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key 		string
	Value 		string
	OpType 		string
	ClientId	int64
	Seq 		int64
}

type TableEntry struct {
	Seq 	int64
	Value	string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// Store KV Pair, Need persist
	store 	   map[string]string 
	table 	   map[int64]*TableEntry
	lastincludeindex int

	indexchan  map[int] chan raft.ApplyMsg
}

func (kv *KVServer) lookUpDupTable(clientid, seq int64) (bool, string) {
	entry, ok := kv.table[clientid]
	if ok {
		s := entry.Seq
		if seq <= s {
			return true, entry.Value
		}
	}
	return false, ""
}

func (kv *KVServer) updateDupTable(clientid, seq int64, value string) {
	entry, exist := kv.table[clientid]
	if exist && entry.Seq >= seq {
		// DPrintf("KVServer %v updateDupTable failed clientid: %v, entry.seq: %v, seq: %v", kv.me, clientid, entry.seq, seq)
		return
	}
	// update dup table
	entry = &TableEntry{}
	entry.Seq = seq
	entry.Value = value
	kv.table[clientid] = entry
	// DPrintf("KVServer %v updateDupTable sucess clientid: %v, seq: %v, value: %v", kv.me, clientid, seq, value)
}

func (kv *KVServer) printvalue() {
	DPrintf("kv store ptr: %p", &kv.store)
	for k := range kv.store {
		DPrintf("KVServer %v Store value, k: %v, value: %v", kv.me, k, kv.store[k])
	}	
}

func (kv *KVServer) getValue(key string) (string, bool) {
	value, exist := kv.store[key]
	kv.printvalue()
	return value, exist
}

func (kv *KVServer) putValue(key, value, op string) {
	if (op == "Put") {
		kv.store[key] = value
	} else {
		str, _ := kv.store[key]
		str = str + value
		kv.store[key] = str
	}
	kv.printvalue()
}

// must get lock first
func (kv *KVServer) notifyRPCHandle(m raft.ApplyMsg) {
 	applychan, exist := kv.indexchan[m.CommandIndex]
	if exist {
		// 注意只有leader才有indexchan
		DPrintf("KVServer %v get applychan for index: %v", kv.me, m.CommandIndex)
		applychan <-m
	} else {
		DPrintf("KVServer %v has no applychan for index: %v", kv.me, m.CommandIndex)
	}
}

func (kv *KVServer) HandleApplyMsg() {
	for  m := range kv.applyCh {
		DPrintf("KVServer %v new ApplyMsg, index: %v", kv.me, m.CommandIndex)
		kv.printvalue()
		if m.CommandValid == false {
			DPrintf("KVserver %v start to restoreFromSnapShot", kv.me)
			kv.restoreFromSnapShot(m.SnapShot)
		} else {
			cmd := m.Command.(Op)
			DPrintf("KVServer %v get ApplyMsg, cmd info: %v, index: %v", kv.me, cmd, m.CommandIndex)
			kv.mu.Lock()
			
			// step2: appy the state
			ok, _ := kv.lookUpDupTable(cmd.ClientId, cmd.Seq)
			if ok {
				// case：在Apply之前dup request已经来了
				DPrintf("KVServer %v donothing because of dup request", kv.me)
				kv.notifyRPCHandle(m)
				kv.mu.Unlock()
				continue
			}
			DPrintf("KVServer %v do unique ApplyMsg, cmd info: %v, index: %v", kv.me, cmd, m.CommandIndex)
			if cmd.OpType != "Get" {
				kv.putValue(cmd.Key, cmd.Value, cmd.OpType)
			}
			value, _ := kv.getValue(cmd.Key)
			DPrintf("KVServer %v update cmd %v's value to %v", kv.me, cmd, value)
			kv.updateDupTable(cmd.ClientId, cmd.Seq, value)
			kv.notifyRPCHandle(m)
			
			// step2: block and make snapshot
			if kv.maxraftstate != -1 && 
			   kv.maxraftstate <= kv.rf.GetPersistSize() {
				lastincludeindex := m.CommandIndex
				DPrintf("KVServer %v start to makeSnapShot, lastincludeindex is %v, len(kv.store): %v, len(kv.table): %v", 
					kv.me, lastincludeindex, len(kv.store), len(kv.table))
				kvdata := kv.makeSnapShot(lastincludeindex)
				DPrintf("KVServer %v makeSnapShot SUCCESS, lastincludeindex is %v", kv.me,  lastincludeindex)
				kv.rf.PersistWithSnapShot(kvdata, lastincludeindex)
			}
			kv.mu.Unlock()
			DPrintf("KVServer %v release to lock", kv.me)
		}
	}
}

func (kv *KVServer) execute(cmd Op, reply *Reply) {
	kv.mu.Lock()
	DPrintf("KVServer %v call Start to excute cmd: %v", kv.me, cmd)
	index, _, is_leader := kv.rf.Start(cmd)			
	DPrintf("KVServer %v start to execute cmd: %v, index: %v, is_leader: %v", kv.me, cmd, index, is_leader)
	if !is_leader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return 
	}
	applyindex := make(chan raft.ApplyMsg)
	DPrintf("KVServer %v new applychan for index: %v, cmd: %v", kv.me, index, cmd)
	kv.indexchan[index] = applyindex
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		DPrintf("KVServer %v delete applychan for index: %v, cmd: %v", kv.me, index, cmd)
		delete(kv.indexchan, index)
		kv.mu.Unlock()
	}()

	DPrintf("KVServer %v block wait for reply of cmd: %v, index: %v", kv.me, cmd, index)
	select {
	case m := <-applyindex:
		cmd_a := m.Command.(Op)
		if m.CommandIndex != index || 
		   cmd.ClientId != cmd_a.ClientId || 
		   cmd.Seq != cmd_a.Seq {
			DPrintf("KVServer %v Err because CommandIndex: %v != index: %v", kv.me, m.CommandIndex, index)
			reply.Err = ErrWrongLeader
			break
		}
		if cmd.OpType == "Get" {
			kv.mu.Lock()
			value, exist := kv.getValue(cmd.Key)
			if !exist {
				reply.Err = ErrNoKey
				kv.mu.Unlock()
				break 
			} else {
				reply.Err = OK
				reply.Value = value
				kv.mu.Unlock()
				break
			}
		} else {
			reply.Err = OK
			break 
		}
	case <-time.After(time.Second):
		reply.Err = ErrWrongLeader
		break
	}
	value , _ := kv.getValue(cmd.Key)
	DPrintf("KVServer %v get reply of cmd: %v, info: %v, value: %v", kv.me, cmd, reply.Err, value)
}

// Assert seq num inc
func (kv *KVServer) Get(args *GetArgs, reply *Reply) {
	// Your code here.
	DPrintf("KVServer %v do Get request and try to get key, args: %v", kv.me, args)
	kv.mu.Lock()
	DPrintf("KVServer %v start to Get %v", kv.me, args)
	// find in dup table
	ok, value := kv.lookUpDupTable(args.ClientId, args.Seq)
	if ok {
		reply.Err = OK
		reply.Value = value
		DPrintf("KVServer %v Get return sucess because dup request, args: %v, value: %v", kv.me, args, value)
		kv.mu.Unlock()
		return 
	}
	cmd := Op{}
	cmd.Key = args.Key
	cmd.OpType = args.Op
	cmd.ClientId = args.ClientId
	cmd.Seq = args.Seq
	kv.mu.Unlock()

	kv.execute(cmd, reply)
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *Reply) {
	// Your code here.
	DPrintf("KVServer %v do Put reques and try to get key, args: %v", kv.me, args)
	kv.mu.Lock()
	DPrintf("KVServer %v start to PutAppend %v", kv.me, args)
	ok, _ := kv.lookUpDupTable(args.ClientId, args.Seq)
	if ok {
		DPrintf("KVServer %v PutAppend return because dup request, args: %v", kv.me, args)
		reply.Err = OK
		kv.mu.Unlock()
		return 
	}
	cmd := Op{}
	cmd.Key = args.Key
	cmd.Value = args.Value
	cmd.OpType = args.Op
	cmd.ClientId = args.ClientId
	cmd.Seq = args.Seq
	kv.mu.Unlock()

	kv.execute(cmd, reply)
}
//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// make sure hold lock when call this func
func (kv *KVServer) makeSnapShot(lastincludeindex int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(lastincludeindex)
	e.Encode(kv.store)
	e.Encode(kv.table)
	data := w.Bytes()
	return data	
}

func (kv *KVServer) restoreFromSnapShot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if (data == nil || len(data) < 1) {
		DPrintf("KVServer %v restoreFromSnapShot failed becaues data is empty", kv.me)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.lastincludeindex) != nil ||
	   d.Decode(&kv.store) != nil ||
	   d.Decode(&kv.table) != nil {
		DPrintf("KVServer %v Decode error, return now", kv.me)
		return 
	}
	DPrintf("KVServer %v restoreFromSnapShot SUCESS, len(kv.store): %v, len(kv.table): %v", 
		kv.me, len(kv.store), len(kv.table))
}


//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	DPrintf("Make new KVServer node %v", me)
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	go kv.HandleApplyMsg()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.table = make(map[int64]*TableEntry)
	kv.indexchan = make(map[int]chan raft.ApplyMsg)
	
	return kv
}
