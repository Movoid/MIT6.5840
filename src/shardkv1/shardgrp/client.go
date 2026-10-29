package shardgrp

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func (self *Clerk) switchLeader() {
	self.leader = (self.leader + 1) % len(self.servers)
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (self *Clerk) Leader() int {
	return self.leader
}

func (self *Clerk) Get(key string) (string, rpc.DataVersion, rpc.Err) {
	// Your code here

	rpcFailedTimes := 0
	args := rpc.GetArgs{Key: key}
	for {
		var reply rpc.GetReply

		ok := self.clnt.Call(self.servers[self.leader], "KVServer.Get", &args, &reply)
		if ok == false {
			rpcFailedTimes++
			if rpcFailedTimes >= 3 {
				return "", rpc.DataVersion(0), rpc.ErrInternalTimeout
			}
			self.switchLeader()
			time.Sleep(time.Duration(100) * time.Millisecond)
			continue
		}

		// Leader 错误, 则在 Group 内找新的.
		if reply.Err == rpc.ErrWrongLeader {
			self.switchLeader()
			time.Sleep(time.Duration(100) * time.Millisecond)
			continue
		}

		// Group 错误, 直接返回, 让上游 Aggregate Clerk 找别的 Group.
		if reply.Err == rpc.ErrWrongGroup {
			return "", reply.Version, rpc.ErrWrongGroup
		}

		// NoKey 错误, 直接返回.
		if reply.Err == rpc.ErrNoKey {
			return "", reply.Version, rpc.ErrNoKey
		}

		if reply.Err != rpc.OK {
			log.Printf("client KVServer.Get RPC received err %v, retry.\n", reply.Err)
			time.Sleep(time.Duration(100) * time.Millisecond)
			continue
		}

		// 成功.
		return reply.Value, reply.Version, rpc.OK
	}
}

func (self *Clerk) Put(key string, value string, version rpc.DataVersion) rpc.Err {
	// Your code here

	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	retried := false
	rpcFailedTimes := 0

	for {
		var reply rpc.PutReply

		ok := self.clnt.Call(self.servers[self.leader], "KVServer.Put", &args, &reply)
		if ok == false {
			rpcFailedTimes++
			if rpcFailedTimes >= 3 {
				return rpc.ErrInternalTimeout
			}
			retried = true
			self.switchLeader()
			time.Sleep(time.Duration(100) * time.Millisecond)
			continue
		}

		// Leader 错误, 在 Group 内找新的.
		if reply.Err == rpc.ErrWrongLeader {
			retried = true
			self.switchLeader()
			time.Sleep(time.Duration(100) * time.Millisecond)
			continue
		}

		// Group 错误, 直接返回, 让上游 Aggregate Clerk 找别的 Group.
		if reply.Err == rpc.ErrWrongGroup {
			return rpc.ErrWrongGroup
		}

		// KV Version 不匹配, 不能执行操作.
		if reply.Err == rpc.ErrVersion {
			if retried {
				return rpc.ErrMaybe
			}
			return rpc.ErrVersion
		}

		if reply.Err != rpc.OK {
			log.Printf("client KVServer.Put RPC received err %v, retry.\n", reply.Err)
			retried = true
			time.Sleep(time.Duration(100) * time.Millisecond)
			continue
		}

		return rpc.OK
	}
}

// Return the server's version even on ErrWrongGroup so the controller can
// distinguish obsolete migration requests during recovery.
// TryFreezeShard makes one attempt, allowing the controller to check current
// between retries if the source group has retired.
func (self *Clerk) TryFreezeShard(s shardcfg.ShardId, num shardcfg.ConfigVersion) (shardrpc.FreezeShardReply, bool) {
	args := shardrpc.FreezeShardArgs{ShardId: s, ConfigVersion: num}
	var reply shardrpc.FreezeShardReply
	ok := self.clnt.Call(self.servers[self.leader], "KVServer.FreezeShard", &args, &reply)
	if !ok || reply.Err == rpc.ErrWrongLeader {
		self.switchLeader()
		return reply, false
	}
	return reply, true
}

func (self *Clerk) FreezeShard(s shardcfg.ShardId, num shardcfg.ConfigVersion) ([]byte, shardcfg.ConfigVersion, rpc.Err) {
	for {
		reply, ok := self.TryFreezeShard(s, num)
		// **IMPORTANT**
		// 如果在 `next` 未完成 config 迁移时,
		// A Controller, B Controller 同时接手任务,
		// 且 A 先完成了迁移并把 `current` 更新为 `next`,
		// 如果之后 B 才开始 `Clerk.FreezeShard()` 等操作,
		// 且 **相关 Shard Group 已经 shutdown**,
		// 则此时 RPC 会失败.
		// 不能在这种情况无限重试.
		// Controller 使用 TryFreezeShard，在重试之间检查已发布的 current。
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return reply.RawShardData, reply.CurConfigVersion, reply.Err
	}
}

// TryInstallShard returns false on communication failure or a wrong leader.
func (self *Clerk) TryInstallShard(s shardcfg.ShardId, state []byte, num shardcfg.ConfigVersion) (shardrpc.InstallShardReply, bool) {
	args := shardrpc.InstallShardArgs{ShardId: s, RawShardData: state, ConfigVersion: num}
	var reply shardrpc.InstallShardReply
	ok := self.clnt.Call(self.servers[self.leader], "KVServer.InstallShard", &args, &reply)
	if !ok || reply.Err == rpc.ErrWrongLeader {
		self.switchLeader()
		return reply, false
	}
	return reply, true
}

func (self *Clerk) InstallShard(s shardcfg.ShardId, state []byte, num shardcfg.ConfigVersion) rpc.Err {
	for {
		reply, ok := self.TryInstallShard(s, state, num)
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return reply.Err
	}
}

// TryDeleteShard returns false on communication failure or a wrong leader.
func (self *Clerk) TryDeleteShard(s shardcfg.ShardId, num shardcfg.ConfigVersion) (shardrpc.DeleteShardReply, bool) {
	args := shardrpc.DeleteShardArgs{ShardId: s, ConfigVersion: num}
	var reply shardrpc.DeleteShardReply
	ok := self.clnt.Call(self.servers[self.leader], "KVServer.DeleteShard", &args, &reply)
	if !ok || reply.Err == rpc.ErrWrongLeader {
		self.switchLeader()
		return reply, false
	}
	return reply, true
}

func (self *Clerk) DeleteShard(s shardcfg.ShardId, num shardcfg.ConfigVersion) rpc.Err {
	for {
		reply, ok := self.TryDeleteShard(s, num)
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return reply.Err
	}
}
