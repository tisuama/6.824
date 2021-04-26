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
// import "bytes"
// import "../labgob"

const MIN_ELECTION_TIMEOUT = 300
const MAX_ELECTION_TIMEOUT = 450
const HB_INTERVAL = 120
const TONANOSECOND = 1000 * 1000

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
	last_applied	  int
	last_log_index    int
	last_log_term	  int

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
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()	
	return rf.term, rf.state == Leader
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
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
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
	LeaderId	 int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	Term         int
	Success      bool
	Status		 AppendEntryStatus
}

// 调用此函数前必须带锁
func (rf *Raft) switchToFollower(term int) {
	DPrintf("server %v switchToFollower, vote_for: %v, term: %v", rf.me, -1, term)
	rf.state = Follower
	rf.term = term
	rf.vote_for = -1
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %v receive RequestVoteRPC, term: %v, args.Term: %v, args.LastLogTerm: %v, args.LastLogIndex: %v", rf.me, rf.term, args.Term, args.LastLogTerm, args.LastLogIndex)
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
		} else {
			// case 1: last_log_term更大
			if rf.log.lastLogTerm() < args.LastLogTerm ||
			   (rf.log.lastLogTerm() == args.LastLogTerm && 
			   rf.log.lastLogIndex() <= args.LastLogIndex) {
				DPrintf("server %v vote for candiate %v", rf.me, args.CandidateId)
				reply.VoteGranted = true
				rf.vote_for = args.CandidateId
				// c) 投票后更新last_hb_time
				rf.last_hb_time = time.Now().UnixNano()
				return
			} else {
				DPrintf("server %v reject to votefor candiate %v because it's log is not up-to-date", rf.me, args.CandidateId)
			}
		}
	}
}

func (rf *Raft) HeartBeat(args  *RequestAppendEntriesArgs, 
						  reply *RequestAppendEntriesReply) {
	
	DPrintf("server %v get heartbeat msg", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %v receive heartbeat args.Term: %v, rf.term: %v", rf.me, args.Term, rf.term)
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		return
	}
	if args.Term > rf.term {
		// 转为Follower状态
		rf.switchToFollower(args.Term)
	}
	// a)收到AppendEntries后重置last_hb_time
	rf.last_hb_time = time.Now().UnixNano()
	rf.updateCommittedIndex(args.LeaderCommit)
	DPrintf("server %v receive heartbeat, reset the election_timeout clock: %v", rf.me, rf.last_hb_time)
	reply.Success = true
	reply.Term = rf.term
}

// 使用此函数之前需要带锁
func (rf *Raft) updateCommittedIndex(committed int ) {

	// 设置commit_index
	if committed > rf.committed_index {
		if committed < rf.log.lastLogIndex() {
			rf.committed_index = committed
		} else {
			rf.committed_index = rf.log.lastLogIndex()
		}
		DPrintf("server %v update committed_index to %v", rf.me, rf.committed_index)
		rf.applyCond.Signal()	
	}
}

func (rf *Raft) AppendEntries(args  *RequestAppendEntriesArgs, 
						  	  reply *RequestAppendEntriesReply) {
	DPrintf("server %v get append entries msg, args.Term: %v, rf.term: %v, args.PrevLogIndex: %v, rf.lastLogIndex: %v, args.PrevLogTerm: %v, rf.lastLogTerm: %v", rf.me, args.Term, rf.term, args.PrevLogIndex, rf.log.lastLogIndex(), args.PrevLogTerm, rf.log.lastLogTerm()) 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	// 会发生args.Term > rf.term嘛
	reply.Term = rf.term
	// 大于当前server term，先将当前server转为Follower
	if args.Term > rf.term {
		rf.switchToFollower(args.Term)
	}
	// 判断异常情况
	if args.Term < rf.term {
		DPrintf("server %v ERROR, LowTermNumber, command: %v", rf.me, args.Entries[0].Command)
		reply.Status = LowTermNumber
		return
	}
	if args.PrevLogIndex > rf.log.lastLogIndex() {
		DPrintf("server %v ERROR, HighLogIndex, command: %v", rf.me, args.Entries[0].Command)
		reply.Status = HighLogIndex
		return
	}
	if rf.log.lastLogTerm() != args.PrevLogTerm {
		DPrintf("server %v ERROR, NotMatchLogTerm, command: %v", rf.me, args.Entries[0].Command)
		reply.Status = NotMatchLogTerm
		return 
	}
	// 对多余的日志进行截断
	if rf.log.truncateSuffix(args.PrevLogIndex) == -1 {
		DPrintf("server %v ERROR, NotFlushToDisk", rf.me)
		reply.Status = NotFlushToDisk
		return
	}
	DPrintf("server %v SUCCESS, command: %v", rf.me, args.Entries[0].Command)
	// Append LogEntry 暂时先不管AppendEntries返回值
	rf.log.appendEntries(args.Entries)
	rf.updateCommittedIndex(args.LeaderCommit)
	// 返回
	reply.Success = true
	reply.Status = OK
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

func (rf *Raft) sendHeartBeatMsg(server int, 
								 args   *RequestAppendEntriesArgs, 
								 reply  *RequestAppendEntriesReply) bool {
	DPrintf("leader %v sendHeartBeatMsg to server %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
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
func (rf *Raft) buildAppendEntriesArgs(server int) (*RequestAppendEntriesArgs, 
									 				*RequestAppendEntriesReply) {
	// buildArgs
	index := rf.next_index[server]
	DPrintf("leader %v's next_index for server %v is %v", rf.me, server, rf.next_index[server])
	// 从leader的log中获取
	entry := rf.log.getEntry(index)
	if entry == nil {
		return nil, nil
	}
	DPrintf("leader %v get LogIndex %v's entry info for server %v, logindex: %v, command: %v", rf.me, index, server, entry.LogIndex, entry.Command)
	request := &RequestAppendEntriesArgs{}
	request.Term = rf.term
	request.LeaderId = rf.me
	request.LeaderCommit = rf.committed_index
	request.Entries = append(request.Entries, entry)
	request.PrevLogTerm = rf.log.getLogTerm(index - 1) 
	request.PrevLogIndex = rf.log.getLogIndex(index - 1)
	
	reply := &RequestAppendEntriesReply{}
	return request, reply
}


func (rf *Raft) doAppendEntriesRPC(server, index int) {
	rf.mu.Lock()
	if rf.next_index[server] != index {
		rf.mu.Unlock()
		DPrintf("server %v return because next_index = %v don't match index = %v", server, rf.next_index[server], index)
		return;
	}
	request, reply := rf.buildAppendEntriesArgs(server)
	if request == nil || reply == nil {
		// 细节
		if rf.next_index[server] > 0 {
			rf.next_index[server]--
			DPrintf("leader %v next_index for server %v sub 1 because get no entry", rf.me, server)
			go rf.doAppendEntriesRPC(server, rf.next_index[server])
		}
		rf.mu.Unlock()
		return 
	}
	DPrintf("leader %v buildAppendEntriesArgs to %v, LogIndex: %v", rf.me, server, request.Entries[0].LogIndex)
	rf.mu.Unlock()
	// send AppendEntriesRPC
	ok := rf.sendAppendEntriesRPC(server, request, reply)
	DPrintf("leader %v get AppendEntriesRPC reply from server %v, ok: %v, Status: %v", rf.me, server, ok, reply.Status)
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
			DPrintf("leader %v do serve %v's append entries reply, reply.Status is OK, command: %v", rf.me, server, request.Entries[0].Command)
			rf.next_index[server]++
			rf.match_index[server] = request.PrevLogIndex + len(request.Entries)
			// Todo: 找到最大的N，leader和Follower的committed_index有不同的更新方式，
			// 更新后都需要通过Cond触发ApplyMsg的操作，应用到StateMachine
			cur_committed_index := rf.match_index[server]
			// 大于当前committed_index才有更新价值
			DPrintf("leader %v process result, server: %v, next_index: %v, match_index: %v, cur_committed_index: %v, rf.committed_index: %v", rf.me, server, rf.next_index[server], rf.match_index[server], cur_committed_index, rf.committed_index)
			if cur_committed_index > rf.committed_index {
				count := 0
				for idx, _ := range rf.peers{
					if rf.match_index[idx] >= cur_committed_index {
						count++
						DPrintf("leader %v inc count to %v because server %v's match_index is %v and cur_committed_index is %v", rf.me, count, idx, rf.match_index[idx], cur_committed_index)
					}
				}
				if count > len(rf.peers) / 2 {
					if cur_committed_index > rf.committed_index {
						rf.committed_index = cur_committed_index
						rf.applyCond.Signal()
						DPrintf("leader %v update committed_index to %v because count is %v  and send applyCond Signal", rf.me, rf.committed_index, count)
					}
				}
			}
			if rf.next_index[server] <= rf.log.lastLogIndex() {
				DPrintf("leader %v comtinue to sendAppendEntriesRPC info to server %v because the next_index for server is %v, but leader's lastLogIndex is %v", rf.me, server, rf.next_index[server], rf.log.lastLogIndex())
				// next_index开始增加
				go rf.doAppendEntriesRPC(server, rf.next_index[server])
			}
		case LowTermNumber, HighLogIndex, NotFlushToDisk, NotMatchLogTerm:
			DPrintf("server %v do server %v's append entries reply, reply.Status is ERROR", rf.me, server)
			// 出现问题，先将next_index减一，后面再优化
			if rf.next_index[server] > 0 {
				rf.next_index[server]--
				go rf.doAppendEntriesRPC(server, index - 1)
			} else {
				return 
			}
		default:
			DPrintf("append entries reply error")
		}
		return 
	} else { // 问题：server重新上线后总是没有返回值？
		DPrintf("server %v do server %v's append entries reply, RPC ERROR", rf.me, server)
		go rf.doAppendEntriesRPC(server, index)
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
	DPrintf("server %v Start, command: %v", rf.me, command)
	entry := &LogEntry{}
	entry.Term = rf.term
	entry.Command = command
	// 更新leader自身的match_index
	entry.LogIndex = rf.next_index[rf.me]
	entries := make([]*LogEntry, 0)
	entries = append(entries, entry)

	// add to laeders log
	rf.log.appendEntry(entry)
	// 更新leader自身的match_index
	rf.next_index[rf.me]++
	// 细节
	rf.match_index[rf.me] = rf.log.lastLogIndex()
	DPrintf("update self match index to %v", rf.match_index[rf.me])

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		// Todo：这里加一个index下标参数
		DPrintf("leader %v start doAppendEntriesRPC for server %v, index: %v, command: %v", rf.me, idx, rf.next_index[idx], command)
		go rf.doAppendEntriesRPC(idx, last_log_index + 1)
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
		for rf.last_applied >= rf.committed_index {
			rf.applyCond.Wait()
		}
		rf.applyCond.L.Unlock()	
		rf.mu.Lock()
		// 每次只对leader的committed_index加一
		DPrintf("server %v try to getEntry %v, last_log_index: %v", rf.me, rf.last_applied + 1, rf.log.lastLogIndex())
		entry := rf.log.getEntry(rf.last_applied + 1)
		DPrintf("server %v try to apply logindex: %v", rf.me, rf.last_applied + 1)
		apply_msg := ApplyMsg{}
		apply_msg.CommandValid = true
		apply_msg.Command = entry.Command 
		apply_msg.CommandIndex = entry.LogIndex
		rf.mu.Unlock()

		// 为了保证日志有序，通过applyCh阻塞发送ApplyMsg
		DPrintf("server %v start to send applymsg, logindex: %v", rf.me, entry.LogIndex)
		rf.applyCh <- apply_msg
		// last_applied只有在当前协程中使用
		rf.last_applied++
		DPrintf("leader %v send applymsg successed", rf.me)
	}
}


func (rf *Raft) startHeartBeat() {
	DPrintf("server %v start heartbeat", rf.me)
	rf.mu.Lock()
	if rf.state == Leader {
		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
		
			request := &RequestAppendEntriesArgs{}
			request.Term = rf.term
			// 附带Leader的committed_index信息
			if rf.match_index[idx] >= rf.committed_index {
				request.LeaderCommit = rf.committed_index
			} else {
				request.LeaderCommit = 0
			}
			// 构造请求
			reply := &RequestAppendEntriesReply{}
			go func(server int) {
				rf.sendHeartBeatMsg(server, request, reply)
				rf.mu.Lock()	
				if reply.Term > rf.term {
					rf.switchToFollower(reply.Term)
				}
				rf.mu.Unlock()
			}(idx)
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
				DPrintf("server %v's sendRequestVote's reply form %v, r.Term: %v, r.VoteGranted: %v, rf.term: %v", rf.me, server, reply.Term, reply.VoteGranted, rf.term)
				// 请求过期了
				if reply.Term < rf.term {
					return
				}
				if reply.Term > request.Term {
					rf.switchToFollower(reply.Term)
				} else {
					if reply.VoteGranted {
						vote_num++
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
						}
						// 一旦append entriesAppendEntriesRPC
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
		if rf.state != Leader {
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
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
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
	rf.log.initLogStorage()
	
	rf.next_index = make([]int, len(rf.peers))
	rf.match_index = make([]int, len(rf.peers))
	rf.committed_index = 0
	rf.last_applied = 0
	rf.applyCond = sync.NewCond(&sync.Mutex{})
	go rf.leaderElectionClock()
	go rf.startSendApplyMsg() 

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
