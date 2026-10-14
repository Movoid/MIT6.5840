# Lab5 思路


## 基本结构


### Shards

shard group 是一组 worker，共同管理若干个 shards.

每个 **shard group 内的 workers 是固定的** .

如, 某个时刻,
group A 中的 worker A1 A2 A3 共同管理 shard 0-5, 
group B 中的 worker B1 B2 B3 共同管理 shard 6-11.

shard group 内的所有 worker 持有这些 shards 的副本,
并使用复制状态机维护.

shard 可以从一个 shard group 迁移到另一个 shard group,
这是 Lab5 需要实现的 **配置变更操作** 中的主要操作.


### Config

Lab5 不需要实现集群成员变更, 不需要实现联合共识.

Config 描述了每个 shard group 包含的 worker,
以及每个 shard group 管理的 shards.

Lab5 需要复用 Lab2 的 trivial KVServer,
用其存储 config .


### Config Server

如果 Config Server 挂掉, 且 Client 没有实现 config 缓存,
则集群不可用, 类似 HBase.

在 [src/shardkv1/shardctrler/shardctrler.go] 里通过 `kvtest.IKVClerk` 访问这个 Config Server .


### Client Workflow

Client 的 KV 操作, 首先基于 Key 计算出 KV 所属的 shard,
之后查询 config 找到其所属的 shard group,
并进行操作.


### Controller Client

Controller 作为特殊的 Client 存在,
可以执行 config 变更操作.

Controller 故障后, 新的 Controller 需要接受 config 变更操作.


## 配置迁移

首先 Config 存储在 Config Server 里.
先 Freeze 旧的被迁移的 Shards，再 Install 新的 Shards，最后 Delete 旧的已经 Frozen 的 Shards。

问题在于配置迁移由 Controller 完成，
期间如果 Controller 崩溃，新的 Controller 需要重新接手。

Config Server 里必须有类似 WAL 的东西，记录这一次要进行的配置迁移操作，
新的 Controller 重走流程，重新通过 Freeze RPC 拿到 version 然后重新做配置迁移。

> 这也是 [src/shardkv1/shardgrp/server.go] 中 `KVServer.DoOp` 里，
> 支持 Frozen -> Frozen 状态转移的意义.


### 并发配置迁移

shard 迁移是系统自动负载均衡调用的，不是给用户主动调用的。
如果发生了并发 shard 迁移，允许其中某些配置迁移操作不被执行。

或者说，如果当前正在进行某个配置迁移过程，
则新发起的配置迁移会立刻返回。


#### 不能只靠高 version 操作优先进行来保证一致性

假设 shard 最初在 X，编号 8 要迁到 Y，编号 9 要迁到 Z。
即使高 version 的操作优先进行，每一步成功后再更新 Config Server 中维护的 config: 

时间	发生的事情
1	Controller 8 完成 Freeze(X, 8)，取得数据 D。
2	Install(Y, D, 8) 在 Y 提交，Y 变成 Owned。
3	Install 的回复丢失，或 Controller 在更新 Config Server 之前暂停。
4	Config Server 仍记录：源组 X 已冻结，目标 Y 尚未确认安装。
5	Controller 9 根据这个记录，从 X 取得冻结数据 D，安装到 Z。

在时间 4 之后，时间 5 之前，Z 还没有 Install，此时如果 Y Install 的消息到达，那么 Config Server 指向 Y，然后 Y 上 Shard 有数据更新。
之后 Z Installed，之后 Config Server 指向 Z，导致数据丢失。这是个问题。

> 本质上是分布式事务问题.
> 尝试用简单办法解决.


### Group 修改

不只是移动 Shard，
Config 可能还会 **添加/删除 Shard Group** .

> 新配置配置可能包含当前配置中不存在的新分片组旧配置，可能排除已存在的碎片组。
> 控制器应该移动分片（键/值数据），以便每个存储的分片集合。分片组与新配置匹配。

Group 的移除，同时伴随着将其 Shards balance 到其他 Groups,
所以不必担心 Agg Clerk 因没有及时更新 cached config 而访问到被删除的 Group,
因为被删除的 Group 的 Shards 都已经被 Freeze，这会返回 ErrWrongGroup 然后触发 sync config.
