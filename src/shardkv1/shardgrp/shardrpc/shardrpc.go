package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

type FreezeShardArgs struct {
	ShardId       shardcfg.ShardId
	ConfigVersion shardcfg.ConfigVersion
}

type FreezeShardReply struct {
	RawShardData     []byte // 返回 Shard.
	CurConfigVersion shardcfg.ConfigVersion
	Err              rpc.Err
}

type InstallShardArgs struct {
	ShardId       shardcfg.ShardId
	RawShardData  []byte // 发送 Shard.
	ConfigVersion shardcfg.ConfigVersion
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	ShardId       shardcfg.ShardId
	ConfigVersion shardcfg.ConfigVersion
}

type DeleteShardReply struct {
	Err rpc.Err
}
