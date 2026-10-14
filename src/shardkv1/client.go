package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

// 这是个 Aggregate Clerk.
// 聚合了用于访问每个 shard group 的 clerk,
// 在这个 layer 计算 key hash 然后选择一个 clerk 进行操作.
type Clerk struct {
	clnt         *tester.Clnt
	controller   *shardctrler.ShardCtrler
	clerkOfGroup map[tester.ShardGroupId]*shardgrp.Clerk
	// You will have to modify this struct.

	cachedConfig *shardcfg.ShardConfig
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, controller *shardctrler.ShardCtrler) kvtest.IKVClerk {
	aggClerk := &Clerk{
		clnt:       clnt,
		controller: controller,
	}
	aggClerk.clerkOfGroup = make(map[tester.ShardGroupId]*shardgrp.Clerk)
	// You'll have to add code here.

	config := controller.Query()
	aggClerk.cachedConfig = config
	for groupId, nameArr := range config.NameArrOfGroup {
		aggClerk.clerkOfGroup[groupId] = shardgrp.MakeClerk(clnt, nameArr)
	}

	return aggClerk
}

func (self *Clerk) GetClerk(gid tester.ShardGroupId) (*shardgrp.Clerk, bool) {
	rck, ok := self.clerkOfGroup[gid]
	return rck, ok
}

// 由于可能有 Shard Group 被加入/删除,
// 需要修改 `self.clerkOfGroup` .
func (self *Clerk) syncConfig() {
	config := self.controller.Query()
	self.cachedConfig = config

	for groupId := range self.clerkOfGroup {
		if _, isExisted := config.NameArrOfGroup[groupId]; isExisted == false {
			delete(self.clerkOfGroup, groupId)
		}
	}

	// 不允许 Group Id 复用,
	// 始终假设相同 Group Id 指向的是同一个 Group, 即使删除后重新加入 config.
	// 此处忽略对此前已加入过的 Group Id 的 clerk 的更新.
	for groupId, nameArr := range config.NameArrOfGroup {
		if _, isExisted := self.clerkOfGroup[groupId]; !isExisted {
			self.clerkOfGroup[groupId] = shardgrp.MakeClerk(self.clnt, nameArr)
		}
	}
}

// 走 [notes](src/shardkv1/notes.md) 中的 Client Workflow.
// 首先计算 hash, 之后查 config server 找到 shard group,
// 然后操作对应这个 shard group 的 clerk 来访问.

// 如果要使用 cached config 的话,
// 考虑 Group 2 被移除, 但 cached config 还未更新的情况,
// 此时继续使用 `self.clerkOfGroup` 中 Group 2 的 clerk,
// 其可能因为 group 内所有 worker shutdown 而无限 RPC 超时失败重试,
// 上游 Agg Clerk 也无法 sync config 来避免此情况.

// 引入 `rpc.ErrInternalTimeout` 处理此情况.
// Group clerk RPC 失败多次后返回此问题, 指示上游 sync config.

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (self *Clerk) Get(key string) (string, rpc.DataVersion, rpc.Err) {

	// Group Clerk 可能返回 WrongGroup NoKey

	for {
		groupId := self.cachedConfig.GroupIdOfShard[shardcfg.ShardIdOfKey(key)]
		groupClerk := self.clerkOfGroup[groupId]

		val, version, err := groupClerk.Get(key)
		if err == rpc.ErrWrongGroup || err == rpc.ErrInternalTimeout {
			self.syncConfig()
			time.Sleep(time.Duration(100) * time.Millisecond)
			continue
		}

		if err == rpc.ErrNoKey {
			return "", version, rpc.ErrNoKey
		}

		if err != rpc.OK {
			log.Fatalf("`Clerk.Get` failed: %v\n", err)
		}

		return val, version, rpc.OK
	}
}

// Put a key to a shard group.
func (self *Clerk) Put(key string, value string, version rpc.DataVersion) rpc.Err {

	// Group Clerk 可能返回 WrongGroup ErrVersion ErrMaybe

	// 需要注意的是 **跨 Group 重试 Put** 的 Err 语义细节.
	// 如果对 A Group 的 Put 成功, 但没有及时收到 RPC Reply,
	// 之后 Shard 被迁移到 B Group, 最后向 B Group 重试 Put,
	// 得到的 `rpc.ErrVersion` 实际需要被当作 `rpc.ErrMaybe` 处理.

	retried := false
	for {
		groupId := self.cachedConfig.GroupIdOfShard[shardcfg.ShardIdOfKey(key)]
		groupClerk := self.clerkOfGroup[groupId]

		err := groupClerk.Put(key, value, version)
		if err == rpc.ErrWrongGroup || err == rpc.ErrInternalTimeout {
			retried = true
			self.syncConfig()
			time.Sleep(time.Duration(100) * time.Millisecond)
			continue
		}

		// **IMPORTANT**
		// 使用 `retried` 处理跨 Group 重试.
		if err == rpc.ErrVersion {
			if retried {
				return rpc.ErrMaybe
			}
			return rpc.ErrVersion
		}

		if err == rpc.ErrMaybe {
			return rpc.ErrMaybe
		}

		if err != rpc.OK {
			log.Fatalf("`Clerk.Put` failed: %v\n", err)
		}

		return rpc.OK
	}

}
