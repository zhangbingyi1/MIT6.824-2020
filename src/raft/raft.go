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

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

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

const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

type LogEntry struct {
	Term int
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

	currentTerm int
	state       string //当前节点状态
	votedFor    int    //该次选举的投票对象
	log         []LogEntry
	expireTime  time.Time //follower、candidate进行投票请求的时间间隔, 或者leader发送心跳的间隔

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
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
	Term         int
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
	Term        int
	VoteGranted bool // true 代表 candidate 获得了该节点投票
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server-%d RequestVote to server-%d ", args.CandidateId, rf.me)

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//符合选举条件
	rf.currentTerm = args.Term
	rf.state = FOLLOWER
	rf.votedFor = args.CandidateId
	rf.genExpireTime()
	reply.VoteGranted = true
	reply.Term = args.Term
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntries struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) HeartBeat(args *AppendEntries, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	DPrintf("server-%d sendHeartBeat to server-%d ", args.LeaderId, rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term
	rf.state = FOLLOWER
	rf.genExpireTime()
	reply.Success = true
	reply.Term = args.Term
	return
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
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
	DPrintf("enter Make server")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.genExpireTime()

	rf.commitIndex = 0
	rf.lastApplied = 0
	DPrintf("go run")
	go rf.run()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) run() {
	for !rf.killed() {

		if !rf.isTimeout() {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if rf.state == LEADER {
			// 定时发送心跳
			rf.heartBeat()
		} else if rf.state == FOLLOWER {
			// 开始发起投票
			DPrintf("server-%d FOLLOWER->CANDIDATE in Term %d", rf.me, rf.currentTerm)
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.mu.Unlock()

			rf.requestVote()
		} else {
			rf.requestVote()
		}
	}
}

func (rf *Raft) requestVote() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.genExpireTime()
	var count int32 = 1 //记录投票数量
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	rf.mu.Unlock()

	var wg sync.WaitGroup

	// 向每个peer发送RPC，通过协程处理
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			wg.Add(1)
			defer wg.Done()
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)
			if !ok {
				DPrintf("rpc failed")
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 发现更大的term，变为follower
			if reply.Term > rf.currentTerm {
				DPrintf("server-%d %s -> FOLLOWER in Term %d, currentTerm=%d", rf.me, rf.state, reply.Term, rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.genExpireTime()
				return
			}

			voteCount := 0
			// 通过投票，计数加1（需要使用原子计数）
			if reply.VoteGranted && rf.state == CANDIDATE {
				voteCount = int(atomic.AddInt32(&count, 1)) //原子操作
			}

			// 收到的计数过半，成为leader，发送心跳给其他服务器
			if int(voteCount) > len(rf.peers)/2 {
				DPrintf("server-%d %s -> LEADER in Term %d", rf.me, rf.state, rf.currentTerm)
				rf.state = LEADER
				rf.heartBeat()
			}
		}(i)
	}
	wg.Wait()
}

func (rf *Raft) heartBeat() {
	rf.genExpireTime()
	args := &AppendEntries{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	var wg sync.WaitGroup

	for i, _ := range rf.peers {

		if i == rf.me {
			continue
		}

		go func(i int) {
			wg.Add(1)
			defer wg.Done()
			reply := &AppendEntriesReply{}
			ok := rf.sendHeartBeat(i, args, reply)
			if !ok {
				DPrintf("rpc failed")
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 收到更大term，变为follower
			if reply.Term > rf.currentTerm {
				DPrintf("server-%d %s -> FOLLOWER in Term %d", rf.me, rf.state, reply.Term)
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.genExpireTime()
			}
		}(i)
	}
	wg.Wait()
}

func (rf *Raft) isTimeout() bool {
	if rf.expireTime.After(time.Now()) {
		return false
	}
	return true
}

func (rf *Raft) genExpireTime() {
	switch rf.state {
	case FOLLOWER, CANDIDATE:
		rf.expireTime = time.Now().Add(time.Duration(200+rand.Intn(100)) * time.Millisecond)
	case LEADER:
		rf.expireTime = time.Now().Add(time.Duration(100) * time.Millisecond)
	}
	//DPrintf("expireTime = %s", rf.expireTime)
}
