# 基于 Raft 的分布式键值存储系统

该代码的 Raft 实现是基于 [etcd](https://github.com/etcd-io/etcd) 的 [raft](https://github.com/etcd-io/etcd/tree/master/raft) 进行修改编写的。

## 功能与实现

- 支持 Put/Get/Del 基础操作
- 实现了 Raft 的基础功能进行容错
- 编程语言采用 Golang 1.13.4

## 服务端

```bash
go build -v .
```

## 客户端

客户端代码在目录 `kvdbctl` 中

```bash
cd kvdbctl
go build -v .
```

## 使用示例

首先安装 [goreman](https://github.com/mattn/goreman)，然后执行

```bash
goreman start
```

会在本地启动 3 个服务端节点组成集群。

使用客户端 `kvdbctl` 向其中一个服务端节点发起 Put 和 Get 请求。

```bash
kvdbctl --endpoint 127.0.0.1:12380 put abc def
ok

kvdbctl --endpoint 127.0.0.1:22380 get abc
abc
def
```
