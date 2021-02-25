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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	//2A
	command interface{}
	term    int
	index   int
}

const (
	Leader    = 1
	Follower  = 2
	Candidate = 3
)

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

	//2A
	currentTerm     int
	log             []Entry
	votedFor        int
	electionTimeout [2]int64
	timer           int64
	role            int
	votedNum        int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Leader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//2A
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
	//2A
	Term        int
	VoteGranted bool
}

func (rf *Raft) initToFollower(term int) {
	rf.currentTerm = term
	rf.role = Follower
	rf.votedFor = -1
	rf.resetTimer()

	rf.debug("initToFollower term %v", rf.currentTerm)
}

func (rf *Raft) debug(s string, args ...interface{}) {
	DPrintf("[%v] [t:%v] %s", rf.me, rf.currentTerm, fmt.Sprintf(s, args...))
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debug("requestVote args %+v", args)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.initToFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())) {
		rf.votedFor = args.CandidateId
		rf.resetTimer()
		reply.VoteGranted = true
		return
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
	rf.debug("sendRequestVote server %v args %+v", server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	//2A
	Term        int
	LeaderId    int
	PreLogIndex int
	PreLogTerm  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	//2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	rf.debug("appendEntries args %+v", args)

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.initToFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	if len(rf.log) < args.PreLogIndex {
		return
	}

	if rf.log[args.PreLogIndex].term != args.PreLogTerm {
		return
	}

	reply.Success = true
	rf.resetTimer()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleAppendReply(reply AppendEntriesReply) {
	//2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debug("handleAppendReply reply %+v", reply)

	if reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.initToFollower(reply.Term)
		return
	}
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
	isLeader := false

	// Your code here (2B).
	//2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == Leader {
		rf.log = append(rf.log, Entry{command: command, term: rf.currentTerm, index: rf.lastLogIndex() + 1})
		index = rf.lastLogIndex()
		term = rf.currentTerm
		isLeader = true
	}

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

func (rf *Raft) handleVoteReply(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debug("handleVoteReply reply %+v", reply)

	if reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.initToFollower(reply.Term)
		return
	}

	if rf.role != Candidate {
		return
	}

	if reply.VoteGranted {
		rf.votedNum++

		if rf.votedNum > len(rf.peers)/2 {
			rf.role = Leader
			rf.debug("i am leader")
			args := AppendEntriesArgs{PreLogIndex: rf.lastLogIndex(), PreLogTerm: rf.lastLogTerm(), Term: rf.currentTerm, LeaderId: rf.me}
			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				go func(server int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					ok := rf.sendAppendEntries(server, &args, &reply)
					if ok {
						rf.handleAppendReply(reply)
					}
				}(server, args)
			}
		}
	}

}

func (rf *Raft) lastLogIndex() int {
	return rf.log[len(rf.log)-1].index
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].term
}

func (rf *Raft) timeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.resetTimer()

	rf.debug("timeout")

	if rf.role != Leader {
		rf.role = Candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.votedNum = 1

		rf.debug("i am Candidate")

		args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastLogIndex(), LastLogTerm: rf.lastLogTerm()}

		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, &args, &reply)
				if ok {
					rf.handleVoteReply(reply)
				}
			}(server, args)
		}
	}
}

func (rf *Raft) resetTimer() {
	rf.timer = nowMs() + rand.Int63n(rf.electionTimeout[1]-rf.electionTimeout[0]) + rf.electionTimeout[0]
	rf.debug("resetTimer %v", rf.timer)
}

func (rf *Raft) heartsbeats() {
	for rf.killed() == false {

		time.Sleep(time.Millisecond * time.Duration(200))

		//2A
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role != Leader {
				return
			}

			rf.debug("heartsbeats")

			args := AppendEntriesArgs{PreLogIndex: rf.lastLogIndex(), PreLogTerm: rf.lastLogTerm(), Term: rf.currentTerm, LeaderId: rf.me}

			for server := range rf.peers {
				if server == rf.me {
					continue
				}

				go func(server int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					ok := rf.sendAppendEntries(server, &args, &reply)
					if ok {
						rf.handleAppendReply(reply)
					}
				}(server, args)
			}
		}()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Millisecond * time.Duration(10))

		//2A
		rf.mu.Lock()
		timer := rf.timer
		rf.mu.Unlock()

		now := nowMs()
		//rf.debug("ticker now %v timer %v", now, timer)
		if now > timer {
			rf.timeout()
		}
	}
}

//millisecond
func nowMs() int64 {
	return time.Now().UnixNano() / 1e6
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
	//2A
	rf.log = []Entry{{command: nil, term: 0, index: 0}}
	rf.electionTimeout[0] = 300
	rf.electionTimeout[1] = 450
	rf.initToFollower(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	//2A
	go rf.heartsbeats()

	return rf
}
