package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

type FreezeShardArgs struct {
	Shard shardcfg.ShardId
	Num   shardcfg.ConfigVersion
}

type FreezeShardReply struct {
	State []byte
	Num   shardcfg.ConfigVersion
	Err   rpc.Err
}

type InstallShardArgs struct {
	Shard shardcfg.ShardId
	State []byte
	Num   shardcfg.ConfigVersion
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	Shard shardcfg.ShardId
	Num   shardcfg.ConfigVersion
}

type DeleteShardReply struct {
	Err rpc.Err
}
