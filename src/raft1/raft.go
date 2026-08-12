package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Term       int
	LogicIndex int
	Command    any
}

type WorkerState int

const (
	Follower WorkerState = iota
	Leader
	Candidate
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int // init with 0
	votedFor    int // init with -1
	leaderId    int // init with -1
	state       WorkerState

	stateIsLeader              *sync.Cond
	stateIsFollowerOrCandidate *sync.Cond

	lastElectionResetTimepoint time.Time
	electionTimeoutDuration    time.Duration

	log []LogEntry // `log[0]` is sentinel.

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

func (self *Raft) resetElectionTimerUnsafe() {
	self.lastElectionResetTimepoint = time.Now()
	self.electionTimeoutDuration = time.Duration(400+rand.Int63n(400)) * time.Millisecond
}

func (self *Raft) transToBeFollowerUnsafe() {
	self.state = Follower
	if self.leaderId == self.me {
		self.leaderId = -1
	}
	self.stateIsFollowerOrCandidate.Signal()
}

func (self *Raft) transToBeCandidateUnsafe() {
	self.state = Candidate
	self.stateIsFollowerOrCandidate.Signal()
}

func (self *Raft) updateTermUnsafe(term int) {
	self.currentTerm = term
	self.votedFor = -1
}

func (self *Raft) rememberLeaderIdUnsafe(id int) {
	self.leaderId = id
}

func (self *Raft) transToBeLeaderUnsafe() {
	self.state = Leader
	self.leaderId = self.me
	self.stateIsLeader.Broadcast() // Active heartbeat.
}

type AppendEntriesArg struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (self *Raft) AppendEntries(arg *AppendEntriesArg, reply *AppendEntriesReply) {

	self.mu.Lock()
	defer self.mu.Unlock()

	// General term check.
	if arg.Term > self.currentTerm {
		self.updateTermUnsafe(arg.Term)
		self.transToBeFollowerUnsafe()
		self.rememberLeaderIdUnsafe(arg.LeaderId)
	}

	// Special check for candidate.
	if arg.Term == self.currentTerm && self.state == Candidate {
		self.transToBeFollowerUnsafe()
		self.rememberLeaderIdUnsafe(arg.LeaderId)
	}

	// Check term.
	reply.Term = self.currentTerm
	if arg.Term < self.currentTerm {
		reply.Success = false
		return
	}

	self.resetElectionTimerUnsafe()

	// Hearbeat?
	if len(arg.Entries) == 0 {
		reply.Success = true
		return
	}

	panic("not implemented.")

}

func (self *Raft) sendAppendEntries(server int, args *AppendEntriesArg, reply *AppendEntriesReply) bool {
	ok := self.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (self *Raft) GetState() (int, bool) {
	self.mu.Lock()
	defer self.mu.Unlock()
	return self.currentTerm, self.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (self *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (self *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (self *Raft) PersistBytes() int {
	self.mu.Lock()
	defer self.mu.Unlock()
	return self.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (self *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArg struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (self *Raft) RequestVote(arg *RequestVoteArg, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	self.mu.Lock()
	defer self.mu.Unlock()

	// General term check.
	if arg.Term > self.currentTerm {
		self.updateTermUnsafe(arg.Term)
		self.transToBeFollowerUnsafe()
	}

	// Check term.
	reply.Term = self.currentTerm
	if arg.Term < self.currentTerm {
		reply.VoteGranted = false
		return
	}

	// Check voted.
	if self.votedFor != -1 && self.votedFor != arg.CandidateId {
		reply.VoteGranted = false
		return
	}

	// Check log.
	if arg.LastLogTerm < self.log[len(self.log)-1].Term ||
		(arg.LastLogTerm == self.log[len(self.log)-1].Term && arg.LastLogIndex < self.log[len(self.log)-1].LogicIndex) {
		reply.VoteGranted = false
		return
	}

	// 只有被同意的 `RequestVote` RPC 才有 `resetElectionTimerUnsafe()` 的意义.
	self.resetElectionTimerUnsafe()
	reply.VoteGranted = true
	self.votedFor = arg.CandidateId
}

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
func (self *Raft) sendRequestVote(server int, arg *RequestVoteArg, reply *RequestVoteReply) bool {
	ok := self.peers[server].Call("Raft.RequestVote", arg, reply)
	return ok
}

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
func (self *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (self *Raft) startBackgroundHeartbeat() {
	for i := range self.peers {
		if i == self.me {
			continue
		}
		go func(peerId int) {
			for {
				time.Sleep(time.Duration(100) * time.Millisecond)
				self.mu.Lock()
				for self.state != Leader {
					self.stateIsLeader.Wait()
				}
				var arg AppendEntriesArg = AppendEntriesArg{
					Term:         self.currentTerm,
					LeaderId:     self.me,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      nil,
					LeaderCommit: 0,
				}
				self.mu.Unlock()

				go func() {
					var reply AppendEntriesReply
					if ok := self.sendAppendEntries(peerId, &arg, &reply); ok == false {
						return
					}
					self.mu.Lock()
					if reply.Term > self.currentTerm {
						self.updateTermUnsafe(reply.Term)
						self.transToBeFollowerUnsafe()
						self.resetElectionTimerUnsafe()
					}
					self.mu.Unlock()
				}()

			}
		}(i)
	}
}

func (self *Raft) startBackgroundElectionTimer() {
	go func() {
		for {
			self.mu.Lock()
			deadline := self.lastElectionResetTimepoint.Add(self.electionTimeoutDuration)
			self.mu.Unlock()

			now := time.Now()
			if !now.After(deadline) {
				time.Sleep(deadline.Sub(now))
			}

			self.mu.Lock()
			for self.state != Follower && self.state != Candidate {
				self.stateIsFollowerOrCandidate.Wait()
			}
			now = time.Now()
			deadline = self.lastElectionResetTimepoint.Add(self.electionTimeoutDuration)
			if !now.After(deadline) {
				self.mu.Unlock()
				continue
			}

			// Need to start a new leader election.
			self.transToBeCandidateUnsafe()
			self.resetElectionTimerUnsafe()
			self.updateTermUnsafe(self.currentTerm + 1)
			var arg RequestVoteArg = RequestVoteArg{
				Term:         self.currentTerm,
				CandidateId:  self.me,
				LastLogIndex: self.log[len(self.log)-1].LogicIndex,
				LastLogTerm:  self.log[len(self.log)-1].Term,
			}
			self.votedFor = self.me
			self.mu.Unlock()

			go func() {
				majorityCnt := len(self.peers)/2 + 1
				receivedVoteCnt := 1

				// Check single worker cluster.

				if receivedVoteCnt >= majorityCnt {
					self.mu.Lock()
					if self.state == Candidate && self.currentTerm == arg.Term {
						self.transToBeLeaderUnsafe()
					}
					self.mu.Unlock()
					return
				}

				receivedVoteCh := make(chan bool, len(self.peers)-1)

				for i := range self.peers {
					if i == self.me {
						continue
					}
					go func(peerId int) {
						var reply RequestVoteReply
						if ok := self.sendRequestVote(peerId, &arg, &reply); ok == false {
							receivedVoteCh <- false
							return
						}
						if reply.Term > arg.Term {
							self.mu.Lock()
							if reply.Term > self.currentTerm {
								self.updateTermUnsafe(reply.Term)
								self.transToBeFollowerUnsafe()
								self.resetElectionTimerUnsafe()
								self.mu.Unlock()
								receivedVoteCh <- false
								return
							}
							self.mu.Unlock()
						}
						receivedVoteCh <- reply.VoteGranted
					}(i)
				}

				for i := 0; i < len(self.peers)-1; i++ {
					granted := <-receivedVoteCh
					if !granted {
						continue
					}

					receivedVoteCnt++
					if receivedVoteCnt >= majorityCnt {
						self.mu.Lock()
						if self.state == Candidate && self.currentTerm == arg.Term {
							self.transToBeLeaderUnsafe()
							self.rememberLeaderIdUnsafe(self.me)
						}
						self.mu.Unlock()
						return
					}

				}

			}()
		}
	}()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	self := &Raft{}
	self.peers = peers
	self.persister = persister
	self.me = me

	// Your initialization code here (3A, 3B, 3C).
	self.currentTerm = 0
	self.votedFor = -1
	self.leaderId = -1
	self.state = Follower

	self.stateIsLeader = sync.NewCond(&self.mu)
	self.stateIsFollowerOrCandidate = sync.NewCond(&self.mu)

	self.resetElectionTimerUnsafe()

	self.log = []LogEntry{{Term: 0, LogicIndex: 0}}

	// initialize from state persisted before a crash
	self.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections

	self.startBackgroundElectionTimer()
	self.startBackgroundHeartbeat()

	return self
}
