# ColdStore 文档总览

本目录文档按“当前实现优先、目标设计分层保留”的原则重新整理。

## 当前权威文档

| 文档 | 定位 |
|------|------|
| [ARCHITECTURE.md](./ARCHITECTURE.md) | 当前代码基线的组件、职责、联动关系、一致性边界 |
| [DESIGN.md](./DESIGN.md) | 较完整的历史设计文档，包含目标态说明，部分内容可能超前于当前实现 |
| [plans/](./plans/) | 阶段性实施计划与演进记录 |
| [BUILD.md](./BUILD.md) | 构建、静态检查与测试规则：工具链、系统依赖、Docker 流程、完成标准、故障排查（[俄语](./BUILD.ru.md)） |

## 模块设计文档

[modules/](./modules/) 下的文档保留为分层设计参考。它们包含部分目标态内容，例如真实多节点 Raft、完整 SPDK、真实磁带库、Admin Console 和 OpenTelemetry 全量接入。

阅读优先级：

1. 先读 [ARCHITECTURE.md](./ARCHITECTURE.md)，了解当前实现。
2. 再读 [modules/README.md](./modules/README.md)，定位某个模块的目标设计。
3. 涉及实现判断时，以 `crates/*/src` 和 `crates/proto/proto` 为准。

## 当前实现状态摘要

| 组件 | 当前状态 |
|------|----------|
| Gateway | Axum S3 HTTP 网关，全部业务请求转发 Scheduler |
| Scheduler | 业务编排中心，负责 PUT/GET/Restore/List、归档扫描、取回调度 |
| Metadata | 单节点持久化状态机路径；多节点 `persistent_raft` 当前 fail-fast，避免伪集群 |
| Cache | staging/restored 双区缓存，支持容量预算、restored 淘汰、staging 背压 |
| Tape | TapeService 抽象与模拟/基础接口，真实设备集成仍是目标态 |
| Proto | gRPC 契约已包含 `CompleteArchiveObject` 原子归档提交接口 |

## 重要一致性约束

- `Staging` 是写入路径数据，不允许被容量淘汰；超预算时必须背压。
- `Restored` 是可重建缓存，可以按 LRU/LFU/TTL/容量回收。
- Scheduler 归档完成必须通过 Metadata 的 `CompleteArchiveObject` 单命令提交，不能拆成多个无条件 RPC。
- Metadata 当前默认单节点，默认 worker metadata 地址也为单节点。
- 多节点 metadata 复制尚未完成，不能将 `persistent_raft` 多节点配置视为生产可用。
