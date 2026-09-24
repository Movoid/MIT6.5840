# Lab5 思路


## Shards

shard group 是一组 worker，共同管理若干个 shards.

每个 shard group 内的 workers 是固定的.

如, 某个时刻,
group A 中的 worker A1 A2 A3 共同管理 shard 0-5, 
group B 中的 worker B1 B2 B3 共同管理 shard 6-11.

shard group 内的所有 worker 持有这些 shards 的副本,
并使用复制状态机维护.

shard 可以从一个 shard group 迁移到另一个 shard group,
这是 Lab5 需要实现的配置变更操作.


## Config

Lab5 不需要实现集群成员变更, 不需要实现联合共识.

Config 描述了每个 shard group 包含的 worker,
以及每个 shard group 管理的 shards.

Lab5 需要复用 Lab2 的 trivial KVServer,
用其存储 config .


## Client Workflow

Client 的 KV 操作, 首先基于 Key 计算出 KV 所属的 shard,
之后查询 config 找到其所属的 shard group,
并进行操作.


## Controller Client

Controller 作为特殊的 Client 存在,
可以执行 config 变更操作.

Controller 故障后, 新的 Controller 需要接受 config 变更操作.


## Config Server

如果 Config Server 挂掉, 且 Client 没有实现 config 缓存,
则集群不可用, 类似 HBase.
