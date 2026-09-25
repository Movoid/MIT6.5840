package kvraft

import (
	"bytes"
	"log"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type Entry struct {
	Value       string
	NextVersion rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	data map[string]Entry
}

// 傻逼架构.
// 实现了 `StateMachine` 接口的 `KVServer` 需要调用 `rsm.RSM` 的 `RSM.Submit()` 方法来向集群提交任务.
// `RSM.Submit()` 可能失败.
// `rsm.RSM` 需要调用 `KVServer` 的 `KVServer.DoOp` 作为 callback 来 apply 操作到 `StateMachine` .

func (self *KVServer) DoOp(req any) any {
	// Your code here

	// 只需支持 Get Put 两个操作.

	// 在 Raft layer 被 applied 的操作,
	// 在业务上不一定成功.
	// 将失败作为操作的结果.

	switch args := req.(type) {
	case rpc.GetArgs:
		entry, ok := self.data[args.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: entry.Value, Version: entry.NextVersion, Err: rpc.OK}

	case rpc.PutArgs:
		entry, ok := self.data[args.Key]
		if !ok {
			if args.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
		} else if entry.NextVersion != args.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}

		self.data[args.Key] = Entry{Value: args.Value, NextVersion: entry.NextVersion + 1}
		return rpc.PutReply{Err: rpc.OK}

	default:
		log.Fatalf("`KVServer.DoOp` received unknown req type: %v\n", req)
	}
	return nil
}

func (self *KVServer) Snapshot() []byte {
	// RSM serializes Snapshot, Restore, and DoOp in its reader.
	var buf bytes.Buffer
	if err := labgob.NewEncoder(&buf).Encode(self.data); err != nil {
		log.Fatalf("KVServer.Snapshot: encode failed: %v", err)
	}
	return buf.Bytes()
}

func (self *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}
	var restored map[string]Entry
	if err := labgob.NewDecoder(bytes.NewReader(data)).Decode(&restored); err != nil {
		log.Fatalf("KVServer.Restore: decode failed: %v", err)
	}
	if restored == nil {
		restored = make(map[string]Entry)
	}
	self.data = restored
}

func (self *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	// 暂时不实现优化的线性一致性读操作.
	// 将读操作单独作为 Log Entry 来 Commit/Apply.

	// `RSM.Submit` 返回错误只可能是 `rpc.ErrWrongLeader`.
	err, res := self.rsm.Submit(*args)
	if err != rpc.OK {
		if err != rpc.ErrWrongLeader {
			log.Fatalf("`KVServer.Get()` received unknown err from `RSM.Submit()`: %v\n", err)
		}
		*reply = rpc.GetReply{
			Value:   "",
			Version: 0,
			Err:     rpc.ErrWrongLeader,
		}
		return
	}

	opRes, ok := res.(rpc.GetReply)
	if ok == false {
		log.Fatalf("`KVServer.Get()` received unknown res from `RSM.Submit()`: %v\n", res)
	}

	*reply = opRes
}

func (self *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	err, res := self.rsm.Submit(*args)
	if err != rpc.OK {
		if err != rpc.ErrWrongLeader {
			log.Fatalf("`KVServer.Put()` received unknown err from `RSM.Submit()`: %v\n", err)
		}
		*reply = rpc.PutReply{
			Err: rpc.ErrWrongLeader,
		}
		return
	}

	opRes, ok := res.(rpc.PutReply)
	if ok == false {
		log.Fatalf("`KVServer.Put()` received unknown res from `RSM.Submit()`: %v\n", res)
	}

	*reply = opRes
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.ShardGroupId, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, data: make(map[string]Entry)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.ShardGroupId, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
