package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
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

	currentTerm int // init with 0. Must persist.
	votedFor    int // init with -1. Must persist.
	leaderId    int // init with -1
	state       WorkerState

	stateIsLeader              *sync.Cond
	stateIsFollowerOrCandidate *sync.Cond

	lastElectionResetTimepoint time.Time
	electionTimeoutDuration    time.Duration

	log []LogEntry // `log[0]` is sentinel. Must persist.

	commitLogicIndex      int
	lastAppliedLogicIndex int

	nextLogicIndex  []int
	matchLogicIndex []int

	applyCh chan raftapi.ApplyMsg

	needApply     *sync.Cond
	needReplicate *sync.Cond
	needCommit    *sync.Cond

	termLogStartLogicIndex map[int]int // 需要恢复 persistent 数据后基于 `log` 重新计算.
}

// ================================================================================

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

	leaderLastLogicIndex := self.log[len(self.log)-1].LogicIndex
	for i := range self.peers {
		self.nextLogicIndex[i] = leaderLastLogicIndex + 1
		self.matchLogicIndex[i] = 0
	}
	// 不要修改 Leader 自己的 `matchLogicIndex` 和 `nextLogicIndex` .
	self.matchLogicIndex[self.me] = leaderLastLogicIndex
	self.nextLogicIndex[self.me] = leaderLastLogicIndex + 1

	self.stateIsLeader.Broadcast() // Active heartbeat.
	self.needReplicate.Broadcast() // Active peer replicators.
}

func (self *Raft) physicalIndexUnsafe(logicIndex int) int {
	return logicIndex - self.log[0].LogicIndex
}

func (self *Raft) maxMajorityMatchLogicIndexUnsafe() int {
	quickSelect := func(slice []int, kth int) int {
		selectSlice := append([]int(nil), slice...)
		kth = max(min(kth, len(selectSlice)-1), 0)
		for {
			l, r := 0, len(selectSlice)-1
			midElem := selectSlice[l+(r-l)/2]
			for l <= r {
				for selectSlice[l] < midElem {
					l++
				}
				for selectSlice[r] > midElem {
					r--
				}
				if l <= r {
					selectSlice[l], selectSlice[r] = selectSlice[r], selectSlice[l]
					l++
					r--
				}
			}
			if r < kth && kth < l {
				return selectSlice[kth]
			}
			if kth < l {
				selectSlice = selectSlice[:r+1]
			} else {
				selectSlice = selectSlice[l:]
				kth -= l
			}
		}
	}
	// **IMPORTANT**
	// Leader 不能直接 commit 当前 `currentTerm` 之前的 Log Entries.
	// 此 Log Index 必须 `isCommitableUnsafe()` 检查后才可 Commit.
	majorityCnt := len(self.peers)/2 + 1
	return quickSelect(self.matchLogicIndex, len(self.peers)-majorityCnt)
}

func (self *Raft) isCommitableUnsafe(logicIndex int) bool {
	return self.currentTerm == self.log[self.physicalIndexUnsafe(logicIndex)].Term
}

// ================================================================================

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

	ConflictTerm                int
	ConflictTermStartLogicIndex int
}

func (self *Raft) AppendEntries(arg *AppendEntriesArg, reply *AppendEntriesReply) {

	self.mu.Lock()
	defer self.mu.Unlock()

	// General term check.
	if arg.Term > self.currentTerm {
		self.updateTermUnsafe(arg.Term)
		self.persistUnsafe()
		self.transToBeFollowerUnsafe()
	}

	// Special check for candidate.
	if arg.Term == self.currentTerm && self.state == Candidate {
		self.transToBeFollowerUnsafe()
	}

	// Check term.
	reply.Term = self.currentTerm
	if arg.Term < self.currentTerm {
		reply.Success = false
		return
	}

	self.rememberLeaderIdUnsafe(arg.LeaderId)
	self.resetElectionTimerUnsafe()

	lastPhysicalLogIndex := func() int { return len(self.log) - 1 }

	// `prevLogIndex` unexists.
	if arg.PrevLogIndex > self.log[lastPhysicalLogIndex()].LogicIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictTermStartLogicIndex = self.log[lastPhysicalLogIndex()].LogicIndex + 1
		return
	}

	if arg.PrevLogIndex < self.log[0].LogicIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictTermStartLogicIndex = self.log[0].LogicIndex + 1
		return
	}

	// `prevLogIndex` exists but term mismatched.
	prevPhysicalLogIndex := arg.PrevLogIndex - self.log[0].LogicIndex
	if self.log[prevPhysicalLogIndex].Term != arg.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = self.log[prevPhysicalLogIndex].Term
		reply.ConflictTermStartLogicIndex = self.termLogStartLogicIndex[self.log[prevPhysicalLogIndex].Term]
		return
	}

	reply.Success = true

	// Check conflict.
	conflictOffset := 0
	haveConflict := false
	for _, newEntry := range arg.Entries {
		curPhysicalIndex := prevPhysicalLogIndex + 1 + conflictOffset
		if curPhysicalIndex >= len(self.log) {
			haveConflict = true
			break
		}
		if self.log[curPhysicalIndex].Term != newEntry.Term {
			haveConflict = true
			break
		}
		conflictOffset++
	}

	if haveConflict {
		conflictPhysicalIndex := prevPhysicalLogIndex + 1 + conflictOffset
		for i := conflictPhysicalIndex; i < len(self.log); i++ {
			curTerm := self.log[i].Term
			if _, ok := self.termLogStartLogicIndex[curTerm]; ok == false {
				continue
			}
			if self.termLogStartLogicIndex[curTerm] == self.log[i].LogicIndex {
				delete(self.termLogStartLogicIndex, curTerm)
			}
		}
		self.log = append(self.log[:conflictPhysicalIndex], arg.Entries[conflictOffset:]...)
		for _, newEntry := range arg.Entries[conflictOffset:] {
			if _, ok := self.termLogStartLogicIndex[newEntry.Term]; ok == false {
				self.termLogStartLogicIndex[newEntry.Term] = newEntry.LogicIndex
			}
		}
		self.persistUnsafe()

	}

	lastMatchLogicIndex := arg.PrevLogIndex
	if len(arg.Entries) > 0 {
		lastMatchLogicIndex = arg.Entries[len(arg.Entries)-1].LogicIndex
	}

	// **IMPORTANT**
	// 必须使用 `AppendEntries` 涉及的最后一个 Log Entry .
	if arg.LeaderCommit > self.commitLogicIndex {
		self.commitLogicIndex = min(arg.LeaderCommit, lastMatchLogicIndex)
		self.needApply.Signal()
	}

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
func (self *Raft) persistUnsafe() {
	var buf bytes.Buffer
	encoder := labgob.NewEncoder(&buf)
	if err := encoder.Encode(self.currentTerm); err != nil {
		log.Fatalf("failed to encode: %v\n", err)
	}
	if err := encoder.Encode(self.votedFor); err != nil {
		log.Fatalf("failed to encode: %v\n", err)
	}
	if err := encoder.Encode(self.log); err != nil {
		log.Fatalf("failed to encode: %v\n", err)
	}

	self.persister.Save(buf.Bytes(), nil)
}

// restore previously persisted state.
func (self *Raft) readPersistUnsafe(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		log.Printf("`readPersistUnsafe` received empty persistent data.\n")
		return
	}

	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)

	var currentTerm int
	var votedFor int
	var entries []LogEntry

	if err := decoder.Decode(&currentTerm); err != nil {
		log.Fatalf("failed to decode: %v\n", err)
	}
	if err := decoder.Decode(&votedFor); err != nil {
		log.Fatalf("failed to decode: %v\n", err)
	}
	if err := decoder.Decode(&entries); err != nil {
		log.Fatalf("failed to decode: %v\n", err)
	}

	self.currentTerm = currentTerm
	self.votedFor = votedFor
	self.log = entries

	self.termLogStartLogicIndex = make(map[int]int)
	for _, entry := range self.log {
		if _, exists := self.termLogStartLogicIndex[entry.Term]; !exists {
			self.termLogStartLogicIndex[entry.Term] = entry.LogicIndex
		}
	}
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
		self.persistUnsafe()
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
	self.persistUnsafe()
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
func (self *Raft) Start(command any) (int, int, bool) {
	self.mu.Lock()
	defer self.mu.Unlock()

	if self.state != Leader {
		return -1, self.currentTerm, false
	}

	entryTerm := self.currentTerm
	entryLogicIndex := self.log[len(self.log)-1].LogicIndex + 1
	self.log = append(self.log, LogEntry{
		Term:       entryTerm,
		LogicIndex: entryLogicIndex,
		Command:    command,
	})

	self.matchLogicIndex[self.me] = entryLogicIndex
	self.nextLogicIndex[self.me] = entryLogicIndex + 1

	if _, ok := self.termLogStartLogicIndex[entryTerm]; ok == false {
		self.termLogStartLogicIndex[entryTerm] = entryLogicIndex
	}
	self.persistUnsafe()

	// 对于单节点的情况,
	// 此时应该 Signal `needCommit` .
	self.needCommit.Signal()
	self.needReplicate.Broadcast()

	return entryLogicIndex, entryTerm, true
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

				// **IMPORTANT**
				// Heartbeart 不处理 Commit 逻辑.
				// 永远发送空 `entries` .
				// 但需要通知 Commit goroutine.
				var arg AppendEntriesArg = AppendEntriesArg{
					Term:         self.currentTerm,
					LeaderId:     self.me,
					PrevLogIndex: self.nextLogicIndex[peerId] - 1,
					PrevLogTerm:  self.log[self.physicalIndexUnsafe(self.nextLogicIndex[peerId]-1)].Term,
					Entries:      []LogEntry(nil),
					LeaderCommit: self.commitLogicIndex,
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
						self.persistUnsafe()
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
			self.persistUnsafe()
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
								self.persistUnsafe()
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

func (self *Raft) startBackgroundLogApplier() {
	go func() {

		for {
			self.mu.Lock()
			for self.commitLogicIndex <= self.lastAppliedLogicIndex {
				self.needApply.Wait()
			}

			i := self.lastAppliedLogicIndex + 1
			var applyMsg raftapi.ApplyMsg = raftapi.ApplyMsg{
				CommandValid: true,
				Command:      self.log[self.physicalIndexUnsafe(i)].Command,
				CommandIndex: i,
			}
			self.lastAppliedLogicIndex++

			self.mu.Unlock()

			self.applyCh <- applyMsg
		}

	}()
}

func (self *Raft) startBackgroundLogReplicator() {

	go func() {

		// **NOTE**
		// Leader 自己推进了 `commitLogicIndex` 后需要 signal 自己的 `needApply` .

		// 检查 `matchLogicIndex`` 并尝试推进 `commitLogicIndex`,
		// 若不能推进则 wait .

		for {
			self.mu.Lock()
			var desiredCommitLogicIndex int
			for {
				desiredCommitLogicIndex = self.maxMajorityMatchLogicIndexUnsafe()
				if self.state == Leader &&
					self.commitLogicIndex < desiredCommitLogicIndex &&
					self.isCommitableUnsafe(desiredCommitLogicIndex) {
					break
				}
				self.needCommit.Wait()
			}

			self.commitLogicIndex = desiredCommitLogicIndex
			self.needApply.Signal()

			// 靠 heartbeat 把新的 `commitLogicIndex` 同步到其他 Followers,
			// 不需要 signal `needReplicate` .

			self.mu.Unlock()
		}

	}()

	for i := range self.peers {
		if i == self.me {
			continue
		}

		go func(peerId int) {
			// 对 Peer 的 Replicator,
			// 使用同步的 `AppendEntires` RPC .

			for {
				self.mu.Lock()
				for {
					if self.state == Leader &&
						self.matchLogicIndex[i] < self.log[len(self.log)-1].LogicIndex {
						break
					}
					self.needReplicate.Wait()
				}

				// **IMPORTANT**
				// 在 Worker 当选 Leader 后,
				// peer 的 `matchLogicIndex` 为 `0`, 但 `nextLogicIndex` 不为 `0` .
				// `matchLogicIndex < leaderLastIndex` 时,
				// 仍可能有 `nextLogicIndex == leaderLastIndex`,
				// 并使得 `AppendEntries` RPC 的 `Entries` 为空.

				peerNextLogicIndex := self.nextLogicIndex[peerId]
				var arg AppendEntriesArg = AppendEntriesArg{
					Term:         self.currentTerm,
					LeaderId:     self.me,
					PrevLogIndex: peerNextLogicIndex - 1,
					PrevLogTerm:  self.log[self.physicalIndexUnsafe(peerNextLogicIndex-1)].Term,
					Entries:      append([]LogEntry(nil), self.log[self.physicalIndexUnsafe(peerNextLogicIndex):]...),
					LeaderCommit: self.commitLogicIndex,
				}
				self.mu.Unlock()

				// **IMPORTANT**
				// `Entries` 可能是空的.

				// 发送 `AppendEntries` RPC 并 **同步** 等待 Reply.
				// 根据 Reply 进行快速回退, 更新 `nextIndex` 并重试.

				var reply AppendEntriesReply

				if ok := self.sendAppendEntries(peerId, &arg, &reply); ok == false {
					continue
				}

				self.mu.Lock()
				if reply.Term > self.currentTerm {
					self.updateTermUnsafe(reply.Term)
					self.persistUnsafe()
					self.transToBeFollowerUnsafe()
					self.resetElectionTimerUnsafe()
					self.mu.Unlock()
					continue
				}

				// 检查当前 Worker 是否仍是 Leader.
				if self.state != Leader || self.currentTerm != arg.Term {
					self.mu.Unlock()
					continue
				}

				// 快速回退
				if !reply.Success {

					if reply.ConflictTerm == -1 {
						self.nextLogicIndex[peerId] = reply.ConflictTermStartLogicIndex
					} else if _, ok := self.termLogStartLogicIndex[reply.ConflictTerm]; ok == false {
						self.nextLogicIndex[peerId] = reply.ConflictTermStartLogicIndex
					} else {
						// 本来 Leader `nextLogicIndex[peerId]` 应该回退到 `reply.ConflictTerm` 的最后一个 Log Index + 1 的位置,
						// 此处为了简单, Leader 仅对此 peer 回退到 `PrevLogTerm` 的第一个 Log Index.
						// self.nextLogicIndex[peerId] = self.termLogStartLogicIndex[arg.PrevLogTerm]

						// 修改:
						// 按标准方法回退到 `ConflictTerm` 的最后一个 Log Index + 1.
						// `newNextLogicIndex` 初始化为 `reply.ConflictTermStartLogicIndex`,
						// 保证至少回退 1.
						newNextLogicIndex := reply.ConflictTermStartLogicIndex

						for i := len(self.log) - 1; i >= 0; i-- {
							if self.log[i].Term == reply.ConflictTerm {
								newNextLogicIndex = self.log[i].LogicIndex + 1
								break
							}
						}

						self.nextLogicIndex[peerId] = newNextLogicIndex
					}

					self.mu.Unlock()
					continue
				}

				lastReplicatedLogicIndex := arg.PrevLogIndex
				if len(arg.Entries) > 0 {
					lastReplicatedLogicIndex = arg.Entries[len(arg.Entries)-1].LogicIndex
				}

				if lastReplicatedLogicIndex > self.matchLogicIndex[peerId] {
					self.matchLogicIndex[peerId] = lastReplicatedLogicIndex
					self.nextLogicIndex[peerId] = lastReplicatedLogicIndex + 1
					self.needCommit.Signal()
				}

				self.mu.Unlock()

			}

		}(i)

	}

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

	self.commitLogicIndex = 0
	self.lastAppliedLogicIndex = 0

	lastLogicIndex := self.log[len(self.log)-1].LogicIndex

	self.nextLogicIndex = make([]int, len(self.peers))
	for i := range self.peers {
		self.nextLogicIndex[i] = lastLogicIndex + 1
	}

	self.matchLogicIndex = make([]int, len(self.peers))

	self.applyCh = applyCh

	self.needApply = sync.NewCond(&self.mu)
	self.needReplicate = sync.NewCond(&self.mu)
	self.needCommit = sync.NewCond(&self.mu)

	self.termLogStartLogicIndex = make(map[int]int)
	self.termLogStartLogicIndex[0] = 0

	// initialize from state persisted before a crash
	self.readPersistUnsafe(persister.ReadRaftState())

	// start ticker goroutine to start elections

	self.startBackgroundElectionTimer()
	self.startBackgroundHeartbeat()
	self.startBackgroundLogApplier()
	self.startBackgroundLogReplicator()

	return self
}
