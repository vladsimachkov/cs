# ColdStore - Rust 实现

S3 兼容的纯冷归档磁带存储系统。

## 项目定位

ColdStore 是**纯冷归档系统**（类似 AWS Glacier Deep Archive）：
- 所有对象写入即标记为 ColdPending，自动排队归档到磁带
- 不提供在线热存储层，不做生命周期管理
- 外部热存储系统在需要冷归档时调用 ColdStore 的 PutObject API

## 当前实现状态

当前代码基线已经进入 Phase 1 本地闭环，并开始 Phase 2A Metadata 单节点持久化：

| 分期 | 状态 | 已落地能力 |
|------|------|------------|
| Phase 1 | 已落地 / 可单测 | Gateway/Scheduler/Metadata/Cache 本地闭环；bucket/object CRUD；Put/Head/Get/Delete/Restore/List；HDD Cache staging/restored；Phase-1 archive 标记 Cold 并清理 staging；TapeService + coldstore-vtl 安全模拟驱动/带库/load/unload/filemark 读写闭环；Scheduler archive batch 可注入 cache+tape writer 并写入 ArchiveBundle/filemark 元数据；CacheArchiveClient 已接入真实 CacheService staging list/get/delete 单测；TapeArchiveClient 已接入真实 TapeService WriteBundle/ReadBundle gRPC 单测 |
| Phase 2A | 已启动 / 可单测 | Metadata opt-in 二进制 snapshot：`MetadataServiceImpl::new_with_snapshot(config, path)` 支持写入后保存、重启后恢复 bucket/object/task/worker/tape 等状态 |
| Phase 2B | 下一步 | 将 Metadata 状态机接入 OpenRaft + RocksDB/openraft-rocksstore，补齐安全的 Raft 状态机单测和多节点一致性测试 |

当前仍不会访问真实磁带设备，也不默认执行集成测试。

## Workspace 结构

| Crate | 类型 | 说明 |
|-------|------|------|
| coldstore-proto | lib | gRPC 协议定义（protobuf 生成） |
| coldstore-common | lib | 共享模型、错误、配置 |
| coldstore-metadata | bin | 元数据集群节点（Raft + RocksDB） |
| coldstore-gateway | bin | S3 HTTP 网关（无状态） |
| coldstore-scheduler | bin | 调度 Worker（业务中枢） |
| coldstore-cache | bin | 缓存 Worker（独立进程，HDD/SPDK） |
| coldstore-tape | bin | 磁带 Worker（独立物理节点） |
| coldstore-vtl | lib | mhVTL/虚拟磁带库 harness：lsscsi/mtx/mt/sg 命令封装 + 安全内存模拟器 |

## 组件间通信

所有组件通过 gRPC 通信，协议定义在 crates/proto/proto/ 下：

- common.proto: 共享枚举和数据结构
- metadata.proto: MetadataService（元数据读写、集群管理、心跳）
- scheduler.proto: SchedulerService（Gateway 转发 S3 请求）
- cache.proto: CacheService（数据暂存和解冻缓存）
- tape.proto: TapeService（磁带读写和驱动管理）

## 快速开始

前置条件: Rust 1.91.1（由 `rust-toolchain.toml` 固定）、protobuf-compiler

在容器内构建（不需要在主机安装 Rust）: `scripts/docker.sh build`，详见 `docs/BUILD.md`。

    make build-debug

分别启动各组件:

    cargo run -p coldstore-metadata
    cargo run -p coldstore-cache
    cargo run -p coldstore-scheduler
    cargo run -p coldstore-gateway

## 许可证

MIT OR Apache-2.0
