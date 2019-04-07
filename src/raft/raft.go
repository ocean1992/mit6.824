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
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
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
	Command      interface{}
	ReceivedTerm int
}

//Server state in RAFT
const (
	Follower  = iota
	Candidate = iota
	Leader    = iota
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	//timeout
	electionTimeout  int
	heartbeatTimeout int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry
	state       int

	//Volatile state on all servers
	commitIndex         int
	lastApplied         int
	latestHeartbeatTime time.Time

	//Volatile state on leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var pCurrentTerm int
	var pVotedFor int
	var pLog []LogEntry
	if d.Decode(&pCurrentTerm) != nil ||
		d.Decode(&pVotedFor) != nil ||
		d.Decode(&pLog) != nil {
		DPrintf("Decode Persist Data Error!\n")
	} else {
		rf.currentTerm = pCurrentTerm
		rf.votedFor = pVotedFor
		rf.log = pLog
	}
}

//
//Trigger of Election
func (rf *Raft) electionTrig() {
	for {

		time.Sleep(time.Duration(rf.electionTimeout) * time.Millisecond)
		switch rf.state {
		case Leader:
		case Follower:
			if time.Now().Sub(rf.latestHeartbeatTime) >= time.Duration(rf.electionTimeout)*time.Millisecond {
				rf.state = Candidate
			}
			go rf.elect()
		case Candidate:
			go rf.elect()
		}
	}
}

//Send HeartBeat (Empty AppendEntries)
func (rf *Raft) leaderWork() {
	DPrintf("%v is leader now!", rf.me)
	replys := make([]AppendEntriesReply, len(rf.peers))
	for rf.state == Leader {
		args := &AppendEntriesArgs{}
		args.LeaderCommit = rf.commitIndex
		args.Term = rf.currentTerm
		args.LeaderID = rf.me

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(peer int) {
				rf.sendAppendEntires(peer, args, &replys[peer])
			}(i)
		}
		time.Sleep(time.Duration(rf.heartbeatTimeout) * time.Millisecond)
	}
}

//A new elect func
func (rf *Raft) elect() {
	if rf.state != Candidate {
		return
	}
	rf.currentTerm++
	rf.votedFor = rf.me
	args := &RequestVoteArgs{}
	args.CandidateID = rf.me
	args.LastLogIndex = len(rf.log) - 1
	if args.LastLogIndex > -1 {
		args.LastLogTerm = rf.log[len(rf.log)-1].ReceivedTerm
	} else {
		args.LastLogTerm = 0
	}
	args.Term = rf.currentTerm
	rf.latestHeartbeatTime = time.Now()

	replys := make([]RequestVoteReply, len(rf.peers))
	oks := make(chan bool, len(rf.peers))
	voteCnt := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			oks <- rf.sendRequestVote(peer, args, &replys[peer])
		}(i)
	}
	for i := 0; i < len(replys)-1; i++ {
		ok := <-oks
		if ok {
			voteCnt++
		}
	}
	if voteCnt > len(rf.peers)/2 && rf.state == Candidate {
		rf.state = Leader
		go rf.leaderWork()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("At term %v, server %v received RequestVote of server %v, args: %v", rf.currentTerm, rf.me, args.CandidateID, *args)
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor > 0 && rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm == args.Term {
		if len(rf.log)-1 <= args.LastLogIndex {
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		//rf.currentTerm < RequestVoteArgs.Term
		reply.VoteGranted = true
	}
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
	return ok && reply.VoteGranted
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("At term %d, server %d received AppendEntries from server %d, args: %v", rf.currentTerm, rf.me, args.LeaderID, args)
	rf.latestHeartbeatTime = time.Now()
	rf.state = Follower
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].ReceivedTerm != args.PrevLogTerm {
			reply.Success = false
		} else {
			rf.currentTerm = args.Term
			reply.Success = true
		}
	}
	//todo: AppendEntries RPC 3. 4. 5. 2A only for heartbeat
	if reply.Success && len(args.Entries) > 0 {
		i := 0
		for ; i < len(args.Entries) && args.PrevLogIndex+i < len(rf.log); i++ {
			if args.Entries[i].ReceivedTerm != rf.log[args.PrevLogIndex+i].ReceivedTerm {
				rf.log = rf.log[0:i]
				break
			}
		}
		for ; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, len(rf.log))
		}
	}
}

func (rf *Raft) sendAppendEntires(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
	if rf.state != Leader {
		return index, term, false
	}
	rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("server %d has been killed", rf.me)
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
	r := rand.New(rand.NewSource(int64(me) * 10))
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.log = make([]LogEntry, 1)
	rf.log[0].ReceivedTerm = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 1
	}

	rf.heartbeatTimeout = r.Intn(200) + 200
	rf.electionTimeout = r.Intn(300) + 300
	DPrintf("server %v electionTimeout is %v", rf.me, rf.electionTimeout)
	rf.latestHeartbeatTime = time.Now()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.electionTrig()
	return rf
}
