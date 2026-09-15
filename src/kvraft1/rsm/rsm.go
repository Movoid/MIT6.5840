package rsm

import (
	"log"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type UniqueOpId struct {
	RsmId int
	OpId  int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id  UniqueOpId
	Req any
}

type CallbackResult struct {
	Id       UniqueOpId
	OpResult any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	rsmId        int
	raft         raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	// Your definitions here.
	nextOpId             int
	callbackOfLogicIndex map[int](chan CallbackResult)
}

func (self *RSM) startBackgroundRsmOpApplier() {

	go func() {

		for applyMsg := range self.applyCh {

			if applyMsg.CommandValid {

				self.mu.Lock()

				op, ok := applyMsg.Command.(Op)
				if ok == false {
					log.Fatalf("RsmOpApplier goroutine received invalid applyMsg type: %v\n", applyMsg)
				}

				opResult := self.sm.DoOp(op.Req)

				// 如果 Op 是其他人发起的,
				// 则当前 Worker 没有注册 callback.
				callback := self.callbackOfLogicIndex[applyMsg.CommandIndex]
				if callback != nil {
					callback <- CallbackResult{Id: op.Id, OpResult: opResult}
					delete(self.callbackOfLogicIndex, applyMsg.CommandIndex)
				}

				self.mu.Unlock()

			} else if applyMsg.SnapshotValid {
				log.Panicf("not implemented.\n")
			} else {
				log.Fatalf("RsmOpApplier goroutine received unknown applyMsg: %v\n", applyMsg)
			}

		}

		log.Printf("applyCh closed. RsmOpApplier goroutine exited.\n")

	}()

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		rsmId:                me,
		maxraftstate:         maxraftstate,
		applyCh:              make(chan raftapi.ApplyMsg),
		sm:                   sm,
		nextOpId:             0,
		callbackOfLogicIndex: make(map[int](chan CallbackResult)),
	}
	if !tester.UseRaftStateMachine {
		rsm.raft = raft.Make(servers, me, persister, rsm.applyCh)
	}

	rsm.startBackgroundRsmOpApplier()

	return rsm
}

func (self *RSM) Raft() raftapi.Raft {
	return self.raft
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (self *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	self.mu.Lock()

	desiredId := UniqueOpId{RsmId: self.rsmId, OpId: self.nextOpId}
	command := Op{
		Id:  desiredId,
		Req: req,
	}
	self.nextOpId++

	opLogicIndex, opTerm, isLeader := self.raft.Start(command)
	if isLeader == false {
		self.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	// 当前 Worker 自认为是 Leader,
	// 不一定真的是集群的 Leader.

	callback := make(chan CallbackResult, 1)

	self.callbackOfLogicIndex[opLogicIndex] = callback

	self.mu.Unlock()

	// 如果操作没能 apply, 比如当前 Worker 已经不再是 Leader,
	// 不能只靠一直等待不匹配的 OpId 来报告失败.
	// 如果集群在此次操作后没有新的操作,
	// 这会导致永久等待.

	// **IMPORTANT**
	// `RSM.Submit()` 返回失败不代表操作一定没有 Commit.
	// Raft Layer 输入 `applyCh` 时没有锁, 这存在一些 Race Window,
	// 如, Op 被 Commit, 在将 Op 成功输入 `applyCh` 前,
	// Worker 就改变了 Term 或不再是 Leader.

	// 对操作只执行一次的保证, 需要在 `StateMachine.DoOp()` 中实现.

	ticker := time.NewTicker(time.Duration(100) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case callbackResult := <-callback:
			if callbackResult.Id != desiredId {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, callbackResult.OpResult

		case <-ticker.C:
			// 如果 Term 已经改变或已经不是 Leader 则直接返回失败.
			if term, isLeader := self.raft.GetState(); term != opTerm || isLeader == false {

				// 必须检查是否仍是 `callback`.
				// 否则可能错误清理了后来的 `RSM.Submit()` 调用注册的 `callback`.
				self.mu.Lock()
				if self.callbackOfLogicIndex[opLogicIndex] == callback {
					delete(self.callbackOfLogicIndex, opLogicIndex)
				}
				self.mu.Unlock()

				return rpc.ErrWrongLeader, nil
			}
			// time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}

}
