package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
// import "net/http"
// import _ "net/http/pprof"
import "bytes"
import "../labgob"

const MIN_ELECTION_TIMEOUT = 200
const MAX_ELECTION_TIMEOUT = 300
const HB_INTERVAL = 100
const TONANOSECOND = 1000 * 1000
const BACKUP_NUMBER = 100

type State int

const (
	Leader State = iota
	Follower
	Candiate
)

type AppendEntryStatus int
const (
	OK AppendEntryStatus = iota
	LowTermNumber
	HighLogIndex
	NotMatchLogTerm
	NotFlushToDisk

)
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// added fileds for snapshot
	SnapShot	 []byte
}

type LogEntry struct {
	Command 	interface{}
	Term 		int
	LogIndex 	int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	// persist state on all servers
	term 	  	      int
	vote_for 		  int
	log		  		  *LogStorage

	// volatile state on all servers
	committed_index   int
	last_committed	  int

	// volatile state on leader 成为leader是初始化
	next_index		  []int // 保存每个server的next_index
	match_index		  []int // 保存每个server的match_index

	// self defined param	
	state 			  State
	election_timeout  int
	last_hb_time      int64

	// Applych
	applyCh 		  chan ApplyMsg
	applyCond		  *sync.Cond	
	last_include_index int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()	
	return rf.term, rf.state == Leader
}

func  (rf *Raft) encodestate() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.vote_for)
	e.Encode(rf.log.getLog())
	e.Encode(rf.log.firstLogIndex())
	data := w.Bytes()
	return data
}

func (rf *Raft) GetPersistSize() int {
	return rf.persister.RaftStateSize()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	// TODO: 持久化first_log_index
	raftstate := rf.encodestate()
	rf.persister.SaveRaftState(raftstate)
	DPrintf("server %v persist complete, term: %v vote_for: %v, lastLogindex: %v", rf.me, rf.term, rf.vote_for, rf.log.lastLogIndex())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(raftstate []byte, snapshot []byte) int{
	if raftstate == nil || len(raftstate) < 1 { // bootstrap without any state?
		DPrintf("[warn]: Server %v readPersist Nothing because data is empty", rf.me)
		return 0
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	var Term int
	var VoteFor int
	var PreLog []*LogEntry
	var index int
	// 解析出raftstate
	r := bytes.NewBuffer(raftstate)
	d := labgob.NewDecoder(r)

	// 解析出lastincludeindex
	sr := bytes.NewBuffer(snapshot)
	sd := labgob.NewDecoder(sr)

	lastincludeindex := 1
	if len(snapshot) != 0 && 
	   sd.Decode(&lastincludeindex) != nil {
		DPrintf("Server %v Decode snapshot info err", rf.me)   
		return 0
	}

	if d.Decode(&Term) != nil ||
	   d.Decode(&VoteFor) != nil ||
	   d.Decode(&PreLog) != nil ||
	   d.Decode(&index) != nil {
		DPrintf("Server %v Decode raftstate info err", rf.me)   
		return 0
   } else {
	    DPrintf("Server %v read persisted state, term: %v, vote_for: %v, lastincludeindex: %v", rf.me, Term, VoteFor, lastincludeindex)
		for idx, logentry := range PreLog {
			DPrintf("Server %v read persisted state, %v-th log info, index: %v, command: %v", rf.me, idx, logentry.LogIndex, logentry.Command)
		}
	   	rf.term = Term
	   	rf.vote_for = VoteFor
		// step1：必须先清空log
		rf.log.resetLog(index)
		DPrintf("Server %v firstLogIndex: %v, lastLogIndex: %v After resetLog", rf.me, rf.log.firstLogIndex(), rf.log.lastLogIndex())
	   	rf.log.appendEntries(PreLog)
		DPrintf("Server %v firstLogIndex: %v, lastLogIndex: %v After AppendEntries", rf.me, rf.log.firstLogIndex(), rf.log.lastLogIndex())
		// step2：更新apply信息
		rf.last_include_index = lastincludeindex
		if rf.last_committed < lastincludeindex {
			rf.last_committed = lastincludeindex - 1
		}
		if rf.committed_index < lastincludeindex {
			rf.committed_index = lastincludeindex
		}
		return lastincludeindex
	}
}

func (rf *Raft) PersistWithSnapShot(kvstate []byte, lastincludeindex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastincludeindex <= rf.last_include_index {
		DPrintf("Server %v PersistWithSnapShot failed becaues lastincludeindex: %v, but rf.last_include_index: %v", 
			rf.me, lastincludeindex, rf.last_include_index)
		return 
	}
	// 坑点：由于PreLogIndex和PreLogTerm的原因，不能用lastincludeindex截断
	DPrintf("Server %v start to truncatePrefix to %v, firstLogIndex: %v, lastLogIndex %v\n", rf.me, lastincludeindex, rf.log.firstLogIndex(), rf.log.lastLogIndex())
	rf.log.truncatePrefix(lastincludeindex)
	DPrintf("Server %v truncatePrefix complete to %v, firstLogIndex: %v, lastLogIndex %v\n", rf.me, lastincludeindex, rf.log.firstLogIndex(), rf.log.lastLogIndex())
	raftstate := rf.encodestate()
	rf.persister.SaveStateAndSnapshot(raftstate, kvstate)
	rf.last_include_index = lastincludeindex
	DPrintf("Server %v PersisWithSnapShot Complete", rf.me)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 		 int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 	 	 int
	VoteGranted  bool	
}


type RequestAppendEntriesArgs struct {
	Term         int
	// 可以优化的点
	LeaderId	 int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
	Status		  AppendEntryStatus
}

type RequestInstallSnapShotArgs struct {
	Term 		int
	RaftState   []byte
	SnapShot 	[]byte
}

type RequestInstallSnapShotReply struct {
	Term	   		 int
	LastIncludeIndex int
	Sucess     		 bool
}

// 调用此函数前必须带锁
func (rf *Raft) switchToFollower(term int) {
	DPrintf("server %v switchToFollower, vote_for: %v, term: %v", rf.me, -1, term)
	rf.state = Follower
	rf.term = term
	rf.vote_for = -1
	rf.persist()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %v receive RequestVoteRPC, term: %v, args.Term: %v, args.LastLogTerm: %v, args.LastLogIndex: %v, rf.lastLogTerm: %v, rf.lastLogIndex: %v", 
		rf.me, rf.term, args.Term, args.LastLogTerm, args.LastLogIndex, rf.log.lastLogTerm(), rf.log.lastLogIndex())
	if args.Term < rf.term {
		DPrintf("server %v reject to vote_for candiate %v because rf.term: %v, args.Term: %v", rf.me, args.CandidateId, rf.term, args.LastLogTerm)
		reply.Term = rf.term
		reply.VoteGranted = false
	} else { 
		if args.Term > rf.term {
			// 转为Follower状态
			DPrintf("server %v switchToFollower because rf.term: %v, args.Term: %v", rf.me, rf.term, args.Term)
			rf.switchToFollower(args.Term)
		}
		// 更新reply的Term
		reply.Term = rf.term
		// 先设为false
		reply.VoteGranted = false
		// 当前Term没票了
		if rf.vote_for != -1 {
			DPrintf("srever %v reject vote_for candiate %v because no ticket for cur term", rf.me, args.CandidateId)
			return
		} else if rf.vote_for == args.CandidateId {
			// 投票RPC丢失的情况
			reply.VoteGranted = true
			rf.last_hb_time = time.Now().UnixNano()
			rf.persist()
			return
		} else {
			// case 1: last_log_term更大
			if rf.log.lastLogTerm() < args.LastLogTerm ||
			   (rf.log.lastLogTerm() == args.LastLogTerm && 
			   rf.log.lastLogIndex() <= args.LastLogIndex) {
				DPrintf("server %v vote for candiate %v", rf.me, args.CandidateId)
				reply.VoteGranted = true
				rf.vote_for = args.CandidateId
				rf.persist()
				// c) 投票后更新last_hb_time
				rf.last_hb_time = time.Now().UnixNano()
				return
			} else {
				DPrintf("server %v reject to votefor candiate %v because it's log is not up-to-date", rf.me, args.CandidateId)
			}
		}
	}
}

// 使用此函数之前需要带锁
func (rf *Raft) updateCommittedIndex(committed int ) {
	
	prev_committed_index := rf.committed_index
	// 设置commit_index
	if committed > rf.committed_index {
		if committed < rf.log.lastLogIndex() {
			rf.committed_index = committed
		} else {
			rf.committed_index = rf.log.lastLogIndex()
		}
		DPrintf("server %v update committed_index to %v, prev_committed_index: %v, commit number: %v", 
			rf.me, rf.committed_index, prev_committed_index, rf.committed_index - prev_committed_index)
		rf.applyCond.Signal()	
	}
}

func (rf *Raft) AppendEntries(args  *RequestAppendEntriesArgs, 
						  	  reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %v get append entries msg, args.Term: %v, rf.term: %v, args.PrevLogIndex: %v, rf.lastLogIndex: %v, args.PrevLogTerm: %v, rf.lastLogTerm: %v, entry count: %v, rf.leaderid: %v", 
		rf.me, args.Term, rf.term, args.PrevLogIndex, rf.log.lastLogIndex(), args.PrevLogTerm, rf.log.lastLogTerm(), len(args.Entries), args.LeaderId) 
	if args.Term == rf.term {
		rf.last_hb_time = time.Now().UnixNano()
		if args.Term == rf.log.lastLogTerm() {
			rf.updateCommittedIndex(args.LeaderCommit)
		}
	}
	reply.Status = OK
	reply.Success = true
	reply.Term = rf.term
	if args.Term > rf.term {
		rf.switchToFollower(args.Term)
	}
	if len(args.Entries) == 0 {
		return 
	}
	// 大于当前server term，先将当前server转为Follower
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	// 判断异常情况
	if args.Term < rf.term {
		DPrintf("server %v ERROR, LowTermNumber, args.Term: %v, rf.term: %v", rf.me, args.Term, rf.term)
		reply.Status = LowTermNumber
		reply.Success = false
		return
	}
	if args.PrevLogIndex > rf.log.lastLogIndex() {
		DPrintf("server %v ERROR, HighLogIndex, prev log index: %v, args log index: %v", 
			rf.me, rf.log.lastLogIndex(), args.PrevLogIndex)
		reply.Status = HighLogIndex
		reply.Success = false
		reply.ConflictIndex = rf.log.lastLogIndex() + 1
		return
	}
	if rf.log.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		DPrintf("server %v ERROR, NotMatchLogTerm, prev log term: %v, args log term: %v", 
			rf.me, rf.log.getLogTerm(args.PrevLogIndex), args.PrevLogTerm)
		reply.Status = NotMatchLogTerm
		reply.Success = false
		reply.ConflictTerm = rf.log.getLogTerm(args.PrevLogIndex)
		reply.ConflictIndex = rf.log.getLogTerm(args.PrevLogIndex)
		for idx := 1; idx <= args.PrevLogIndex; idx++ {
			if rf.log.getLogTerm(idx) == reply.ConflictTerm {
				reply.ConflictIndex = idx
				break
			}
		}
		return 
	}
	DPrintf("server %v's log truncateSuffix to %v", rf.me, args.PrevLogIndex)
	if args.PrevLogIndex + len(args.Entries) <= rf.log.lastLogIndex() && 
		args.Entries[len(args.Entries) -1].Term == rf.log.getLogTerm(args.PrevLogIndex + len(args.Entries)) {
		DPrintf("server %v AppendEntries return because had Appended before", rf.me)
		return 
	}
	// 对多余的日志进行截断
	if rf.log.truncateSuffix(args.PrevLogIndex) == -1 {
		DPrintf("server %v ERROR, NotFlushToDisk", rf.me)
		reply.Status = NotFlushToDisk
		reply.Success = false
		return
	}
	DPrintf("server %v appendEntry SUCCESS, command: %v", rf.me, args.Entries[0].Command)
	// Append LogEntry 暂时先不管AppendEntries返回值
	rf.log.appendEntries(args.Entries)
	rf.persist()
	// 返回
	return 	
}

func (rf *Raft) InstallSnapShot(args  *RequestInstallSnapShotArgs,
							    reply *RequestInstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %v start to InstallSnapShot", rf.me)
	reply.Sucess = true
	reply.Term = rf.term
	if rf.term > args.Term {
		reply.Sucess = false
		return 
	}
	if args.Term > rf.term {
		rf.switchToFollower(args.Term)
	}
	last_include_index := rf.readPersist(args.RaftState, args.SnapShot)
	reply.LastIncludeIndex = last_include_index
	// 坑点： 要安装下
	rf.persister.SaveStateAndSnapshot(args.RaftState, args.SnapShot)
	DPrintf("Server %v InstallSnapShot complete", rf.me)
	return 
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("candiate %v sendRequestVote to server %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


// send是阻塞的，且sned的时候是没加锁的
func (rf *Raft) sendAppendEntriesRPC(server int, 
								   args   *RequestAppendEntriesArgs, 
								   reply  *RequestAppendEntriesReply) bool {
	DPrintf("leader %v sendAppendEntriesRPC to server %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


func (rf *Raft) sendInstallSnapShotRPC(server int, 
								   args   *RequestInstallSnapShotArgs, 
								   reply  *RequestInstallSnapShotReply) bool {
	DPrintf("leader %v sendInstallSnapShotRPC to server %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) buildAppendEntriesArgs(server int, is_heartbeat bool) (*RequestAppendEntriesArgs, 
	*RequestAppendEntriesReply, bool) {
	request := &RequestAppendEntriesArgs{}
	request.Term = rf.term
	request.LeaderId = rf.me
	request.LeaderCommit = rf.committed_index
	reply := &RequestAppendEntriesReply{}
	// buildArgs
	if !is_heartbeat {
		index := rf.next_index[server]
		DPrintf("leader %v's next_index for server %v is %v, firstLogIndex: %v, lastLogIndex: %v", 
			rf.me, server, rf.next_index[server], rf.log.firstLogIndex(), rf.log.lastLogIndex())
		// case 1: log已经被截断时
		if index != 1 && index <= rf.log.firstLogIndex() {
			DPrintf("leader %v buildAppendEntriesArgs for server %v need_snapshot, because index: %v, but firstLogIndex is %v", rf.me, server, index, rf.log.firstLogIndex())
			return request, reply, true		
		} else {
			// case 2: 走正常流程
			// 从leader的log中获取
			entries := rf.log.getEntry(index, BACKUP_NUMBER)
			request.Entries = entries
			request.PrevLogTerm = rf.log.getLogTerm(index - 1) 
			request.PrevLogIndex = rf.log.getLogIndex(index - 1)
		}
	} else {
		request.PrevLogTerm = rf.log.lastLogTerm()	
		request.PrevLogIndex = rf.log.lastLogIndex()
	}
	
	return request, reply, false
}

func (rf *Raft) doInstallSnapShotRPC(server int) {
	rf.mu.Lock()
	request := &RequestInstallSnapShotArgs{}
	request.Term = rf.term
	request.RaftState = rf.persister.ReadRaftState()
	request.SnapShot = rf.persister.ReadSnapshot()
	reply := &RequestInstallSnapShotReply{}
	rf.mu.Unlock()
	ok := rf.sendInstallSnapShotRPC(server, request, reply)	
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.term {
			rf.switchToFollower(reply.Term)
			return 
		}
		if request.Term != rf.term || reply.Sucess == false {
			DPrintf("Server %v doInstallSnapShotRPC ERR, request.Term != rf.term or reply.Sucess is not true", rf.me);
			return
		}
		cur_log_index := reply.LastIncludeIndex
		rf.next_index[server] = cur_log_index + 1
		rf.match_index[server] = cur_log_index
		DPrintf("Leader %v doInstallSnapShotRPC SUCESS, next_index[%v]: %v, match_index[%v]: %v", 
			rf.me, server, rf.next_index[server], server, rf.match_index[server])
		return 
	}
}

func (rf *Raft) doAppendEntriesRPC(server, index int,  is_heartbeat bool) {
	rf.mu.Lock()
	if rf.state != Leader {
		DPrintf("server %v doAppendEntriesRPC fail because not leader now", rf.me)
		rf.mu.Unlock()
		return 
	}
	if !is_heartbeat && rf.next_index[server] != index {
		DPrintf("server %v return because next_index = %v don't match index = %v", server, rf.next_index[server], index)
		rf.mu.Unlock()
		return;
	}
	request, reply, need_snapshot := rf.buildAppendEntriesArgs(server, is_heartbeat)
	if need_snapshot {
		DPrintf("Leader %v need send snapshot to server %v because next_index is %v but firstLogIndex is %v", rf.me, server, rf.next_index[server], rf.log.firstLogIndex())
		rf.mu.Unlock()
		go rf.doInstallSnapShotRPC(server)
		return 
	}
	if len(request.Entries) != 0 {
		DPrintf("Leader %v buildAppendEntriesArgs for server %v, START LogIndex: %v, entry count: %v, is_heartbeat: %v, request info: %v", 
			rf.me, server, request.Entries[0].LogIndex, len(request.Entries), is_heartbeat, request)
	} else {
		DPrintf("Leader %v buildAppendEntriesArgs for server %v with NO ENTRIES, is_heartbeat: %v", rf.me, server, is_heartbeat)
	}
	rf.mu.Unlock()
	// send AppendEntriesRPC
	ok := rf.sendAppendEntriesRPC(server, request, reply)
	DPrintf("leader %v get AppendEntriesRPC reply from server %v, ok: %v, Status: %v, is_heartbeat: %v", 
		rf.me, server, ok, reply.Status, is_heartbeat)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 丢弃了这个包，如果没有，那肯定还是leader
		if request.Term != rf.term {
			DPrintf("leader %v do server %v's append entries reply, request.Term != rf.term", rf.me, server)
			return
		}
		// 收到更大的Term
		if reply.Term > rf.term {
			DPrintf("leader %v do server %v's append entries reply, switchToFollower", rf.me, server)
			rf.switchToFollower(reply.Term)
			return 
		}
		// 成功则处理
		switch reply.Status {
		case OK:
			if !is_heartbeat {
				DPrintf("leader %v do server %v's append entries reply, reply.Status is OK, entry count: %v", rf.me, server, len(request.Entries))
				cur_log_index := request.PrevLogIndex + len(request.Entries)
				if cur_log_index + 1 <= rf.next_index[server] {
					DPrintf("Leader %v doAppendEntriesRPC for server %v complete because request.PrevLogIndex: %v, entry count: %v, rf.next_index: %v", 
						rf.me, server, request.PrevLogIndex, len(request.Entries), rf.next_index[server])
					return 
				}
				rf.next_index[server] = cur_log_index + 1
				rf.match_index[server] = cur_log_index
				// TODO: 找到最大的N，leader和Follower的committed_index有不同的更新方式，
				// 更新后都需要通过Cond触发ApplyMsg的操作，应用到StateMachine
				cur_committed_index := rf.match_index[server]
				// 大于当前committed_index才有更新价值
				DPrintf("leader %v process result, server %v, next_index: %v, last_log_index: %v,  match_index: %v, cur_committed_index: %v, rf.committed_index: %v", rf.me, server, rf.next_index[server], 
					rf.log.lastLogIndex(), rf.match_index[server], cur_committed_index, rf.committed_index)
				if cur_committed_index > rf.committed_index {
					count := 0
					for idx, _ := range rf.peers{
						if rf.match_index[idx] >= cur_committed_index {
							count++
							DPrintf("leader %v inc count to %v because server %v's match_index is %v and cur_committed_index is %v", 
								rf.me, count, idx, rf.match_index[idx], cur_committed_index)
						}
					}
					if count > len(rf.peers) / 2 {
						if cur_committed_index > rf.committed_index && 
							rf.log.getLogTerm(cur_committed_index) == rf.term {
							prev_committed_index := rf.committed_index
							rf.committed_index = cur_committed_index
							rf.applyCond.Signal()
							DPrintf("leader %v update committed_index to %v because count is %v and send applyCond Signal, pre committed index: %v, commit number: %v", 
								rf.me, rf.committed_index, count, prev_committed_index, rf.committed_index - prev_committed_index)
						}
					}
				}
			}
			if rf.next_index[server] <= rf.log.lastLogIndex() {
				DPrintf("leader %v continue to sendAppendEntriesRPC info to server %v because the next_index for server is %v, but leader's lastLogIndex is %v", 
					rf.me, server, rf.next_index[server], rf.log.lastLogIndex())
				// next_index开始增加
				go rf.doAppendEntriesRPC(server, rf.next_index[server], false)
			}
		case LowTermNumber, HighLogIndex, NotFlushToDisk, NotMatchLogTerm:
			DPrintf("server %v do server %v's append entries reply, reply.Status is ERROR", rf.me, server)
			// 出现问题，先将next_index减一，后面再优化
			if reply.ConflictTerm != -1 {
				DPrintf("Leader %v and server %v 's ConflictTerm is %v", rf.me, server, reply.ConflictTerm)
				idx := 0
				has_term := false
				for idx = request.PrevLogIndex; idx > 0; idx-- {
					term := rf.log.getLogTerm(idx)
					if term == reply.ConflictTerm {
						has_term = true
						break
					}
				}
				if has_term {
					rf.next_index[server] = idx + 1
					DPrintf("ConflictTerm SETTING, Leader %v set server %v's next_index to %v", rf.me, server, rf.next_index[server])
					go rf.doAppendEntriesRPC(server, rf.next_index[server], false)
					return	
				} else {
					DPrintf("Leader %v can't find ConflictTerm %v, but ConflictIndex is %v", rf.me, reply.ConflictTerm, reply.ConflictIndex)
				}
			}
			if reply.ConflictIndex != -1 {
				DPrintf("Leader %v and server %v's ConflictIndex is %v", rf.me, server, reply.ConflictIndex)
				rf.next_index[server] = reply.ConflictIndex
				DPrintf("ConflictIndex SETTING, Leaser %v set server's next_index to %v", rf.me, rf.next_index[server])
				go rf.doAppendEntriesRPC(server, rf.next_index[server], false)
				return 
			} 
			if rf.next_index[server] > 1 {
				rf.next_index[server]--
				DPrintf("Leader %v next_index for server %v sub 1 to %v and continue", rf.me, server, rf.next_index[server])
				go rf.doAppendEntriesRPC(server, rf.next_index[server], false)
				return
			}
			DPrintf("Leader %v nothing to do because next_index for server %v is %v", rf.me, server, rf.next_index[server])
		default:
			DPrintf("append entries reply error")
		}
		return 
	} else {
		DPrintf("Leader %v do server %v's append entries reply, RPC ERROR", rf.me, server)
		return 
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	last_log_index := rf.log.lastLogIndex()
	// 细节
	if rf.state != Leader {
		return last_log_index + 1, rf.term, false
	}
	DPrintf("##### Leader %v Start, command: %v", rf.me, command)
	entry := &LogEntry{}
	entry.Term = rf.term
	entry.Command = command
	// 更新leader自身的match_index
	entry.LogIndex = rf.next_index[rf.me]
	entries := make([]*LogEntry, 0)
	entries = append(entries, entry)

	// add to laeders log
	rf.log.appendEntry(entry)
	rf.persist()
	// 更新leader自身的match_index
	rf.next_index[rf.me]++
	// 细节
	rf.match_index[rf.me] = rf.log.lastLogIndex()
	DPrintf("Leader %v update self match index to %v", rf.me, rf.match_index[rf.me])

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		// TODO：这里加一个index下标参数
		DPrintf("leader %v start doAppendEntriesRPC for server %v, index: %v, command: %v", rf.me, idx, rf.next_index[idx], command)
		go rf.doAppendEntriesRPC(idx, last_log_index + 1, false)
	}
	return last_log_index + 1, rf.term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startSendApplyMsg() {
	for true {
		rf.applyCond.L.Lock()
		DPrintf("server %v get applyCond Lock and startSendApplyMsg", rf.me)
		for rf.last_committed >= rf.committed_index {
			rf.applyCond.Wait()
		}
		rf.applyCond.L.Unlock()	
		rf.mu.Lock()
		apply_msg := ApplyMsg{}
		if rf.last_committed >= rf.committed_index {
			rf.mu.Unlock()
			continue;
		} else if rf.last_committed < rf.last_include_index {
			DPrintf("server %v try to send Snapdata by applyCh, rf.last_committed: %v, rf.last_include_index: %v", 
				rf.me, rf.last_committed, rf.last_include_index)
			apply_msg.CommandValid = false
			apply_msg.CommandIndex = rf.last_include_index
			apply_msg.SnapShot = rf.persister.ReadSnapshot()	
		} else {
			DPrintf("server %v try to getEntry %v, first_log_index: %v, last_log_index: %v, rf.committed_index: %v", 
				rf.me, rf.last_committed + 1, rf.log.firstLogIndex(), rf.log.lastLogIndex(), rf.committed_index)
			entry := rf.log.getEntry(rf.last_committed + 1, 1)
			DPrintf("server %v try to apply logindex: %v", rf.me, rf.last_committed + 1)
			apply_msg.CommandValid = true
			apply_msg.Command = entry[0].Command 
			apply_msg.CommandIndex = entry[0].LogIndex			
		}
		rf.mu.Unlock()

		rf.applyCh <- apply_msg
		rf.mu.Lock()
		// last_committed只有在当前协程中使用
		if apply_msg.CommandIndex >= rf.last_include_index {
			rf.last_committed++
		}
		DPrintf("server %v send applymsg successed", rf.me)
		rf.mu.Unlock()
	}
}


func (rf *Raft) startHeartBeat() {
	DPrintf("Leader %v start heartbeat", rf.me)
	rf.mu.Lock()
	if rf.state == Leader {
		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
			go rf.doAppendEntriesRPC(idx, rf.log.lastLogIndex() + 1, true)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) startLeaderElection() {
	DPrintf("server %v start leader election", rf.me)
	vote_num, reply_num := 1, 0
	var wg sync.WaitGroup
	// 构造request，可以通用
	rf.mu.Lock()
	

	rf.term++
	rf.vote_for = rf.me
	DPrintf("server %v term %v votefor itself", rf.me, rf.term)
	rf.state = Candiate
	rf.persist()
	// b)开始选举后重置last_hb_time
	rf.last_hb_time = time.Now().UnixNano()

	request := &RequestVoteArgs{}
	request.CandidateId = rf.me
	request.Term = rf.term
	request.LastLogTerm = rf.log.lastLogTerm()
	request.LastLogIndex = rf.log.lastLogIndex()
	for idx := 0; idx < len(rf.peers); idx++ {
		if idx == rf.me {
			continue
		}
		wg.Add(1)
		reply := &RequestVoteReply{}
		go func(server int) {
			defer wg.Done()
			ok := rf.sendRequestVote(server, request, reply)
			if !ok {
				DPrintf("server %v sendRequestVote back network failure from %v", rf.me, server)
				return
			}	
			rf.mu.Lock()
			reply_num++		
			if ok {
				DPrintf("server %v's sendRequestVote's reply form %v, r.Term: %v, r.VoteGranted: %v, rf.term: %v", 
					rf.me, server, reply.Term, reply.VoteGranted, rf.term)
				// 请求过期了
				if request.Term != rf.term {
					// 细节：忘了Unlock
					rf.mu.Unlock()
					return
				}
				if reply.Term > request.Term {
					rf.switchToFollower(reply.Term)
				} else {
					if reply.VoteGranted {
						vote_num++
						DPrintf("server %v receive vote from server %v and  vote_num is to %v", rf.me, server, vote_num)
					}
				}
				if vote_num > len(rf.peers) / 2 {
					// 判断当前状态是否还是Candiate
					if rf.state == Candiate && vote_num > len(rf.peers) / 2 {
						rf.state = Leader
						DPrintf("server %v become leader, next_index for all server: %v", rf.me, rf.log.lastLogIndex() + 1)
						// 成为leader后重置next_index和match_index
						for idx, _ := range rf.peers {
							rf.next_index[idx] = rf.log.lastLogIndex() + 1
							// 置为-1还是0，有不同的说法
							rf.match_index[idx] = 0
							DPrintf("Leader %v update server %v next_index to %v, and match_index to %v", 
								rf.me, idx, rf.next_index[idx], rf.match_index[idx])
						}
						go rf.heartBeatClock()
					}
				}
				rf.mu.Unlock()
			}
		}(idx)
	}
	rf.mu.Unlock()
	wg.Wait()
	DPrintf("server %v wg done, killed: %v", rf.me, rf.killed())
}


func (rf *Raft) heartBeatClock() {
	DPrintf("server %v start a new heartBeatClock", rf.me)
	for true {
		rf.mu.Lock()
		if rf.state != Leader || rf.killed() == true {
			rf.mu.Unlock()
			DPrintf("server %v stop heartBeatClock", rf.me)
			break
		}
		rf.mu.Unlock()
		go rf.startHeartBeat()
		time.Sleep(time.Duration(HB_INTERVAL) * time.Millisecond)
	}
	DPrintf("server %v is dead: %v, state: %v", rf.me, rf.killed(), rf.state)
}

func (rf *Raft) leaderElectionClock() {
	for true {
		time.Sleep(time.Duration(rf.election_timeout) * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			// 如果是Leader，那就没事了，直接装死
			rf.mu.Unlock()
			continue
		}
		last_time := rf.last_hb_time
		cur_time := time.Now().UnixNano()
		// 超时，开始选举，重置计时
		if cur_time - last_time >= int64(rf.election_timeout) * TONANOSECOND {
			DPrintf("server %v election timeout, cur_time: %v, last_time: %v, interval: %v, election_timeout: %v, Leader ? %v, timeout: %v", 
				rf.me, cur_time, last_time, cur_time - last_time, rf.election_timeout, rf.state == Leader, cur_time - last_time >= int64(rf.election_timeout) * TONANOSECOND)
			go rf.startLeaderElection()
		}
		rf.mu.Unlock()
	}

}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. , prev_committed_index, rf.committed_index - prev_committed_indexapplyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	
	DPrintf("Make new raft node %v", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 后台起一个go routine 周期的通过发送RequestVote RPC开始选举
	rf.applyCh = applyCh
	election_interval := MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT + 1
	rf.election_timeout = MIN_ELECTION_TIMEOUT + rand.Int() % election_interval
	rf.last_hb_time = time.Now().UnixNano()
	rf.vote_for = -1
	rf.state = Follower
	// 初始化LogStorage
	rf.log = &LogStorage{}
	rf.log.initLogStorage(1)
	
	rf.next_index = make([]int, len(rf.peers))
	rf.match_index = make([]int, len(rf.peers))
	rf.committed_index = 0
	rf.last_committed = 0
	rf.last_include_index = 0
	rf.applyCond = sync.NewCond(&sync.Mutex{})
	go rf.leaderElectionClock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	go rf.startSendApplyMsg() 

	// 监控代码
	// go func() {
	// 	http.ListenAndServe("localhost:9876", nil)
	// }()

	return rf
}
