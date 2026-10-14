package shardgrp

import (
	"bytes"
	"log"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type Entry struct {
	Value       string
	NextVersion rpc.DataVersion
}

// 当前实现中, Raft Log 的并行粒度是 Shard group 级别的,
// 给每个 Shard 使用不同 mutex 没意义.

type ShardStatus uint8

const (
	NotOwned ShardStatus = iota
	Owned
	Frozen
)

// 每个 Shard 持有数据.
type Shard struct {
	ShardId       shardcfg.ShardId
	ConfigVersion shardcfg.ConfigVersion
	Status        ShardStatus
	KvData        map[string]Entry
}

// Shard group 中每个 worker 的 KVServer
type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.ShardGroupId

	// Your code here
	shards map[shardcfg.ShardId]Shard
}

func (self *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		shard := self.shards[shardcfg.ShardIdOfKey(args.Key)]
		if shard.Status != Owned {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		entry, ok := shard.KvData[args.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: entry.Value, Version: entry.NextVersion, Err: rpc.OK}

	case rpc.PutArgs:
		shard := self.shards[shardcfg.ShardIdOfKey(args.Key)]
		if shard.Status != Owned {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
		entry, ok := shard.KvData[args.Key]
		if !ok {
			if args.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
		} else if entry.NextVersion != args.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}

		shard.KvData[args.Key] = Entry{Value: args.Value, NextVersion: entry.NextVersion + 1}
		return rpc.PutReply{Err: rpc.OK}

	// 扩展 `KVServer.DoOp` 以支持 config 变更的操作.
	// unexisted 和 not owned 不是相同的 semantics.
	// 对 not owned 状态进行转移, 必须符合 config version 的规则.

	// 下面的 cases 重点在于处理 **重复** Freeze Install Delete 操作.
	// 对于重复的操作, 允许相同的 config version.
	case shardrpc.FreezeShardArgs:
		shard, isExisted := self.shards[args.ShardId]
		if !isExisted {
			return shardrpc.FreezeShardReply{CurConfigVersion: shard.ConfigVersion, Err: rpc.ErrWrongGroup}
		}

		// NotOwned -> Frozen is invalid.
		// Owned -> Frozen is allowed, but need version matched.
		// Frozen -> Frozen is allowed, but need version matched.

		// **IMPORTANT**
		// Frozen -> Frozen 需要 version 严格相等.
		isValidTransform :=
			(shard.Status == Owned && shard.ConfigVersion < args.ConfigVersion) ||
				(shard.Status == Frozen && shard.ConfigVersion == args.ConfigVersion)
		if !isValidTransform {
			return shardrpc.FreezeShardReply{CurConfigVersion: shard.ConfigVersion, Err: rpc.ErrWrongGroup}
		}

		shard.Status = Frozen
		shard.ConfigVersion = args.ConfigVersion
		self.shards[args.ShardId] = shard

		var buf bytes.Buffer
		if err := labgob.NewEncoder(&buf).Encode(shard.KvData); err != nil {
			log.Fatalf("`KVServer.DoOp`, case `shardrpc.FreezeShardArgs`: encode failed: %v\n", err)
		}
		return shardrpc.FreezeShardReply{RawShardData: buf.Bytes(), CurConfigVersion: shard.ConfigVersion, Err: rpc.OK}

	case shardrpc.InstallShardArgs:
		shard, isExisted := self.shards[args.ShardId]
		// This shard should be unexisted.
		if !isExisted {
			return shardrpc.InstallShardReply{Err: rpc.ErrWrongGroup}
		}

		// NotOwned -> Owned is allowed, but need version matched.
		// Owned -> Owned is allowed, but need version matched.
		// Frozen -> Owned is invalid.

		// **IMPORTANT**
		// Owned -> Owned 需要 version 严格相等.
		isValidTransform :=
			(shard.Status == NotOwned && shard.ConfigVersion < args.ConfigVersion) ||
				(shard.Status == Owned && shard.ConfigVersion == args.ConfigVersion)
		if !isValidTransform {
			return shardrpc.InstallShardReply{Err: rpc.ErrWrongGroup}
		}

		// 不能让重复 Install 覆盖已有的 data.
		if shard.Status == Owned {
			return shardrpc.InstallShardReply{Err: rpc.OK}
		}

		var data map[string]Entry
		if err := labgob.NewDecoder(bytes.NewReader(args.RawShardData)).Decode(&data); err != nil {
			log.Fatalf("`KVServer.DoOp`, case `shardrpc.InstallShardArgs`: decode failed: %v\n", err)
		}

		if data == nil {
			data = make(map[string]Entry)
		}

		shard.ConfigVersion = args.ConfigVersion
		shard.Status = Owned
		shard.KvData = data
		self.shards[args.ShardId] = shard
		return shardrpc.InstallShardReply{Err: rpc.OK}

	case shardrpc.DeleteShardArgs:
		shard, isExisted := self.shards[args.ShardId]
		if !isExisted {
			return shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
		}

		// NotOwned -> NotOwned is allowed, but need version matched.
		// Owned -> NotOwned is invalid.
		// Frozen -> NotOwned is allowed, but need version matched.

		// **IMPORTANT**
		// NotOwned -> NotOwned 需要 version 严格相等.
		// Frozen -> NotOwned 需要 version 严格相等.
		isValidTransform :=
			(shard.Status == NotOwned && shard.ConfigVersion == args.ConfigVersion) ||
				(shard.Status == Frozen && shard.ConfigVersion == args.ConfigVersion)
		if !isValidTransform {
			return shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
		}

		shard.ConfigVersion = args.ConfigVersion
		shard.Status = NotOwned
		shard.KvData = nil
		self.shards[args.ShardId] = shard
		return shardrpc.DeleteShardReply{Err: rpc.OK}

	default:
		log.Fatalf("`KVServer.DoOp` received unknown req type: %v\n", req)
	}
	return nil
}

func (self *KVServer) Snapshot() []byte {
	// Your code here
	var buf bytes.Buffer
	if err := labgob.NewEncoder(&buf).Encode(self.shards); err != nil {
		log.Fatalf("KVServer.Snapshot: encode failed: %v", err)
	}
	return buf.Bytes()
}

func (self *KVServer) Restore(data []byte) {
	// Your code here
	if len(data) == 0 {
		return
	}
	var restored map[shardcfg.ShardId]Shard
	if err := labgob.NewDecoder(bytes.NewReader(data)).Decode(&restored); err != nil {
		log.Fatalf("KVServer.Restore: decode failed: %v", err)
	}
	if restored == nil {
		restored = make(map[shardcfg.ShardId]Shard)
	}
	self.shards = restored
}

func (self *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
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
	// Your code here
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

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (self *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, res := self.rsm.Submit(*args)
	if err != rpc.OK {
		if err != rpc.ErrWrongLeader {
			log.Fatalf("`KVServer.FreezeShard()` received unknown err from `RSM.Submit()`: %v\n", err)
		}
		*reply = shardrpc.FreezeShardReply{Err: rpc.ErrWrongLeader}
		return
	}

	opRes, ok := res.(shardrpc.FreezeShardReply)
	if ok == false {
		log.Fatalf("`KVServer.FreezeShard()` received unknown res from `RSM.Submit()`: %v\n", res)
	}
	*reply = opRes
}

// Install the supplied state for the specified shard.
func (self *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, res := self.rsm.Submit(*args)
	if err != rpc.OK {
		if err != rpc.ErrWrongLeader {
			log.Fatalf("`KVServer.InstallShard()` received unknown err from `RSM.Submit()`: %v\n", err)
		}
		*reply = shardrpc.InstallShardReply{Err: rpc.ErrWrongLeader}
		return
	}

	opRes, ok := res.(shardrpc.InstallShardReply)
	if ok == false {
		log.Fatalf("`KVServer.InstallShard()` received unknown res from `RSM.Submit()`: %v\n", res)
	}
	*reply = opRes
}

// Delete the specified shard.
func (self *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, res := self.rsm.Submit(*args)
	if err != rpc.OK {
		if err != rpc.ErrWrongLeader {
			log.Fatalf("`KVServer.DeleteShard()` received unknown err from `RSM.Submit()`: %v\n", err)
		}
		*reply = shardrpc.DeleteShardReply{Err: rpc.ErrWrongLeader}
		return
	}

	opRes, ok := res.(shardrpc.DeleteShardReply)
	if ok == false {
		log.Fatalf("`KVServer.DeleteShard()` received unknown res from `RSM.Submit()`: %v\n", res)
	}
	*reply = opRes
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.ShardGroupId, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	// 所有的 shard 首先都属于 `shardcfg.Gid1` shard group.
	// 否则处于 `NotOwned` 状态.
	kv := &KVServer{gid: gid, me: me, shards: make(map[shardcfg.ShardId]Shard)}
	for id := shardcfg.ShardId(0); id < shardcfg.NShards; id++ {
		shard := Shard{ShardId: id, Status: NotOwned}
		if gid == shardcfg.Gid1 {
			shard.Status = Owned
			shard.ConfigVersion = shardcfg.NumFirst
			shard.KvData = make(map[string]Entry)
		}
		kv.shards[id] = shard
	}
	// Initialize before MakeRSM: startup Restore replaces this initial state.
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.ShardGroupId, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
