package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// Lab5 中,
// 集群的 config 在一个特殊 Server (Lab2 实现的 trivial KVServer) 上维护,
// 使用 `ShardCtrler` 这个特化 Client 来访问.

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clntForConfigServer  *tester.Clnt
	clerkForConfigServer kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	controller := &ShardCtrler{clntForConfigServer: clnt}

	// 使用 Lab2 `kvsrv.MakeClerk()`, 复用 Lab2 的业务重试等逻辑来操作 clnt.
	srv := tester.ServerName(tester.GRP0, 0)
	controller.clerkForConfigServer = kvsrv.MakeClerk(clnt, srv)
	return controller
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (self *ShardCtrler) InitController() {

}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (self *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	rawCfg := cfg.String()
	err := self.clerkForConfigServer.Put("config", rawCfg, 0)

	// 需要处理 `rpc.ErrMaybe`,
	// 可能第一次成功的 Put Reply 丢失, 重试后得到 ErrMaybe.
	if err == rpc.ErrMaybe {
		savedRawConfig, _, readErr := self.clerkForConfigServer.Get("config")
		if readErr == rpc.OK && savedRawConfig == rawCfg {
			return
		}
		// Config Server 已有不同的 config? 意外错误.
		log.Fatalf("`ShardCtrler.InitConfig` failed. the config server had a conflict config.\n")
	}
	if err != rpc.OK {
		log.Fatalf("`ShardCtrler.InitConfig` failed: %v\n", err)
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
// **IMPORTANT**
// `ShardCtrler.ChangeConfigTo` 可以静默失败.
func (self *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	raw, version, err := self.clerkForConfigServer.Get("config")
	if err != rpc.OK {
		log.Fatalf("`ShardCtrler.ChangeConfigTo` read config failed: %v", err)
	}
	old := shardcfg.FromString(raw)
	if new.ConfigVersion <= old.ConfigVersion {
		return
	}

	// Part A: configuration changes are invoked sequentially. Validate all
	// endpoints before freezing any shard, and use the old directory for sources.
	for shard, source := range old.GroupIdOfShard {
		destination := new.GroupIdOfShard[shard]
		if source != destination && (len(old.NameArrOfGroup[source]) == 0 || len(new.NameArrOfGroup[destination]) == 0) {
			return
		}
	}

	sources := make(map[tester.ShardGroupId]*shardgrp.Clerk)
	destinations := make(map[tester.ShardGroupId]*shardgrp.Clerk)
	for shard, source := range old.GroupIdOfShard {
		destination := new.GroupIdOfShard[shard]
		if source == destination {
			continue
		}
		if sources[source] == nil {
			sources[source] = shardgrp.MakeClerk(self.clntForConfigServer, old.NameArrOfGroup[source])
		}
		if destinations[destination] == nil {
			destinations[destination] = shardgrp.MakeClerk(self.clntForConfigServer, new.NameArrOfGroup[destination])
		}
		id := shardcfg.ShardId(shard)
		state, _, err := sources[source].FreezeShard(id, new.ConfigVersion)
		if err != rpc.OK {
			return
		}
		if err := destinations[destination].InstallShard(id, state, new.ConfigVersion); err != rpc.OK {
			return
		}
		if err := sources[source].DeleteShard(id, new.ConfigVersion); err != rpc.OK {
			return
		}
	}

	// Publish only after every move completes. A lost Put reply can produce
	// ErrMaybe; read back before deciding whether another attempt is needed.
	target := new.String()
	for {
		err := self.clerkForConfigServer.Put("config", target, version)
		if err == rpc.OK || err == rpc.ErrVersion {
			return
		}
		if err != rpc.ErrMaybe {
			log.Fatalf("`ShardCtrler.ChangeConfigTo` publish failed: %v", err)
		}
		current, currentVersion, err := self.clerkForConfigServer.Get("config")
		if err != rpc.OK {
			log.Fatalf("`ShardCtrler.ChangeConfigTo` verify config failed: %v", err)
		}
		if current == target || currentVersion != version {
			return
		}
	}
}

// Return the current configuration
func (self *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	var rawCfg, _, err = self.clerkForConfigServer.Get("config")
	if err != rpc.OK {
		log.Fatalf("`ShardCtrler.Query` failed: %v\n", err)
	}
	cfg := shardcfg.FromString(rawCfg)
	return cfg
}
