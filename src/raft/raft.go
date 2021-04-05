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
	term 	  	      int64
	vote_for 		  int
	state 			  State
	election_timeout  int
	last_hb_time      int64

	// 先用这俩代替
	last_log_index    int64
	last_log_term     int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()	
	return int(rf.term), bool(rf.state == Leader)
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
	Term 		 int64
	CandidateId  int
	LastLogIndex int64
	LastLogTerm  int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 	 	 int64
	VoteGranted  bool	
}


type RequestAppendEntriesArgs struct {
	Term         int64
	LeaderId	 int
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []*LogEntry
	LeaderCommit int64
}

type RequestAppendEntriesReply struct {
	Term         int64
	Success      bool
}

// 调用此函数前必须带锁
func (rf *Raft) switchToFollower(term int64) {
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
			DPrintf("server %v switchToFollower because rf.term: %v, args.LastLogTerm: %v", rf.me, rf.term, args.LastLogTerm)
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
			// Todo
			if rf.last_log_term < args.LastLogTerm ||
			   (rf.last_log_term == args.LastLogTerm && 
			   rf.last_log_index <= args.LastLogIndex) {
				reply.VoteGranted = true
				rf.vote_for = args.CandidateId
				// c) 投票后更新last_hb_time
				rf.last_hb_time = time.Now().UnixNano()
				return
			}
		}
	}
}

func (rf *Raft) HandleHeartBeatMsg(args  *RequestAppendEntriesArgs, 
								   reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	DPrintf("server %v received heartbeat, reset the election_timeout clock: %v", rf.me, rf.last_hb_time)
	reply.Success = true
	reply.Term = rf.term
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
	ok := rf.peers[server].Call("Raft.HandleHeartBeatMsg", args, reply)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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

func (rf *Raft) startHeartBeat() {
	DPrintf("server: %v start heartbeat", rf.me)
	var wg sync.WaitGroup
	reply_res := make([]*RequestAppendEntriesReply, 0, 10)	
	rf.mu.Lock()
	request := &RequestAppendEntriesArgs{}
	request.Term = rf.term
	if rf.state == Leader {
		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
			wg.Add(1)
			// 构造请求
			reply := &RequestAppendEntriesReply{}
			reply_res = append(reply_res, reply)
			go func(server int) {
				defer wg.Done()
				rf.sendHeartBeatMsg(server, request, reply)
			}(idx)
		}
	}
	rf.mu.Unlock()
	wg.Wait()
	rf.mu.Lock()
	for _, r := range reply_res {
		if r.Term > rf.term {
			rf.switchToFollower(r.Term)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) startLeaderElection() {
	DPrintf("server %v start leader election", rf.me)
	var wg sync.WaitGroup
	reply_res := make([]*RequestVoteReply, 0, 10)	
	// 构造request，可以通用
	rf.mu.Lock()
	

	rf.term++
	rf.vote_for = rf.me
	rf.state = Candiate
	// b)开始选举后重置last_hb_time
	rf.last_hb_time = time.Now().UnixNano()

	request := &RequestVoteArgs{}
	request.CandidateId = rf.me
	request.Term = rf.term
	// Todo
	// request.LastLogTerm
	// request.LastLogIndex
	for idx := 0; idx < len(rf.peers); idx++ {
		if idx == rf.me {
			continue
		}
		wg.Add(1)
		reply := &RequestVoteReply{}
		reply_res = append(reply_res, reply)
		go func(server int) {
			defer wg.Done()
			rf.sendRequestVote(server, request, reply)
		}(idx)
	}
	rf.mu.Unlock()
	wg.Wait()
	vote_num := 1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candiate {
		return 
	}
	for _, r := range reply_res {
		DPrintf("vote info: r.Term: %v, r.VoteGranted: %v", r.Term, r.VoteGranted)
		if r.Term > request.Term {
			// 转为Follower状态
			rf.switchToFollower(r.Term)
		} else if r.VoteGranted {
			vote_num++
		}
	}
	DPrintf("server: %v, state: %v, vote_num: %v", rf.me, rf.state, vote_num)
	// 判断当前状态是否还是candiate
	if rf.state == Candiate && vote_num > len(rf.peers) / 2 {
		rf.state = Leader
		DPrintf("server: %v become leader", rf.me)
		// 一旦选举成功，发送empty AppendEntriesRPC
		go rf.heartBeatClock()
	}
}


func (rf *Raft) heartBeatClock() {
	for true {
		go rf.startHeartBeat()
		time.Sleep(time.Duration(HB_INTERVAL) * time.Millisecond)
	}
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
		if cur_time - last_time >= int64(rf.election_timeout * TONANOSECOND) {
			DPrintf("server %v election timeout, cur_time: %v, last_time: %v, interval: %v, election_timeout: %v", 
				rf.me, cur_time, last_time, cur_time - last_time, rf.election_timeout)
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
	election_interval := MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT + 1
	rf.election_timeout = MIN_ELECTION_TIMEOUT + rand.Int() % election_interval
	rf.last_hb_time = time.Now().UnixNano()
	rf.vote_for = -1
	rf.state = Follower
	go rf.leaderElectionClock()


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
