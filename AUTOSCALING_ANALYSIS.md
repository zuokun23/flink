# Flink 自动扩缩容机制深度分析

## 1. 总体概述

Flink 在自动扩缩容方面的工作主要涉及两大机制：

1. **Adaptive Scheduler（自适应调度器）** — 用于**流处理**作业，能根据可用资源动态调整作业并行度。这是 Flink 原生的、最核心的自动扩缩容实现，基于 FLIP-160 设计。
2. **Adaptive Batch Scheduler（自适应批处理调度器）** — 用于**批处理**作业，根据上游算子产出的数据量在调度时动态决定下游算子的并行度。

此外，还有一个特殊的 **Reactive Mode（被动模式）**，是 Adaptive Scheduler 的一种执行模式，允许作业根据集群中可用的 TaskManager 数量自动将并行度扩展到最大值。

核心代码位于：
- `flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/adaptive/` — 自适应调度器（约 60 个文件）
- `flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/adaptivebatch/` — 自适应批调度器（约 34 个文件）
- `flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/slotpool/` — 声明式资源池

---

## 2. Adaptive Scheduler（自适应调度器）

### 2.1 设计理念（FLIP-160）

传统的 `DefaultScheduler` 是静态的：在提交作业时必须已知所有资源，如果资源不足作业就会挂起等待。`AdaptiveScheduler` 则不同：

- **声明式资源管理**：不再命令式地请求特定数量的 Slot，而是"声明"所需资源，由 ResourceManager 尽力满足
- **弹性并行度**：根据实际可用 Slot 数量，自动计算每个算子可运行的最大并行度
- **运行时扩缩**：作业在运行过程中，如果资源增加或减少，可以动态触发 rescale（通过 checkpoint 恢复重启实现）

### 2.2 状态机架构

AdaptiveScheduler 的核心是一个**有限状态机**，定义了 8 个状态：

```
                        ┌──────────────┐
                        │   Created    │ (初始状态)
                        └──────┬───────┘
                               │ startScheduling()
                               ▼
                    ┌─────────────────────┐
            ┌──────│ WaitingForResources  │◀────────┐
            │      └──────────┬──────────┘          │
            │                 │ 资源充足             │ 资源不足
            │                 ▼                     │
            │    ┌───────────────────────┐          │
            │    │ CreatingExecutionGraph│──────────┘
            │    └──────────┬───────────┘ (Slot分配失败)
            │               │ 创建成功 + Slot分配成功
            │               ▼
            │       ┌──────────────┐        扩缩/故障
            │       │  Executing   │──────────────────┐
            │       └──────┬───────┘                  │
            │              │                          ▼
            │              │               ┌──────────────┐
            │              │               │  Restarting   │
            │              │               └───────┬──────┘
            │              │                       │
            │              │            ┌──────────┴──────────┐
            │              │            │                     │
            │              │     ┌──────▼───────┐    ┌───────▼──────────┐
            │              │     │WaitingFor    │    │CreatingExecution │
            │              │     │Resources     │    │Graph             │
            │              │     └──────────────┘    └──────────────────┘
            │              │
            │      ┌───────┴──────┐    ┌──────────┐
            │      │StopWithSave- │    │ Canceling │
            │      │  point       │    └─────┬─────┘
            │      └──────────────┘          │
            │                                ▼
            │                        ┌──────────────┐
            └───────────────────────▶│   Finished   │ (终态)
                                     └──────────────┘

     另有 Failing 状态（不可恢复故障时进入）
```

每个状态都实现了 `State` 接口，关键状态的职责：

| 状态 | 职责 |
|------|------|
| `Created` | 初始状态，调用 `startScheduling()` 后转入 `WaitingForResources` |
| `WaitingForResources` | 等待资源到位，有超时机制，资源充足则转入 `CreatingExecutionGraph` |
| `CreatingExecutionGraph` | 异步创建 ExecutionGraph（根据可用资源调整并行度），分配 Slot |
| `Executing` | **核心状态**：作业运行中，持续监听资源变化，判断是否需要 rescale |
| `Restarting` | 取消当前 ExecutionGraph，等待所有 Task 停止后，决定进入 `WaitingForResources` 或直接 `CreatingExecutionGraph` |
| `Canceling` / `Failing` / `Finished` | 终止流程 |
| `StopWithSavepoint` | 带 Savepoint 停止 |

### 2.3 源码关键类

```
AdaptiveScheduler.java                     (1716 行) — 调度器主类，实现 SchedulerNG + 所有状态的 Context
├── Settings                               — 封装所有配置参数
├── StateTransitionManagerFactory          — 状态转换管理器工厂
│
State.java                                 — 状态接口
├── Created.java                           — 初始状态
├── WaitingForResources.java               — 等待资源
├── CreatingExecutionGraph.java            — 创建执行图
├── Executing.java                         — 执行中（核心）
├── Restarting.java                        — 重启中
├── Canceling.java                         — 取消中
├── Failing.java                           — 失败处理
├── Finished.java                          — 终态
├── StopWithSavepoint.java                 — Savepoint 停止
│
StateTransitions.java                      — 定义所有合法的状态转换接口
StateTransitionManager.java                — 状态转换决策接口
DefaultStateTransitionManager.java         — 默认实现（含 Phase 状态机）
ResourceListener.java                      — 资源变化监听接口
│
allocator/
├── SlotAllocator.java                     — Slot 分配器接口
├── SlotSharingSlotAllocator.java          — 支持 Slot 共享的分配器实现
├── VertexParallelism.java                 — 各顶点的并行度方案
├── StateLocalitySlotAssigner.java         — 基于状态局部性的 Slot 分配
├── StateSizeEstimates.java                — 状态大小估算
├── JobAllocationsInformation.java         — 作业分配历史信息
├── SlotsBalancedSlotMatchingResolver.java — 均衡 Slot 匹配
├── TasksBalancedSlotMatchingResolver.java — Task 均衡匹配
│
timeline/
├── Rescale.java                           — Rescale 事件详细记录（FLIP-495）
├── TriggerCause.java                      — Rescale 触发原因
├── VertexParallelismRescale.java          — 顶点并行度变化记录
├── SlotSharingGroupRescale.java           — Slot 变化记录
├── SchedulerStateSpan.java                — 调度器状态耗时记录
```

---

## 3. 扩缩容决策机制

### 3.1 DefaultStateTransitionManager（资源变化决策状态机）

`Executing` 状态在运行过程中，持续监控资源变化。是否触发 rescale 由 `DefaultStateTransitionManager` 决定，它本身也是一个**内部状态机**，包含 5 个 Phase：

```
Cooldown → Idling → Stabilizing → Stabilized → Transitioning
```

| Phase | 说明 |
|-------|------|
| **Cooldown** | 冷却期：上次 rescale 完成后的等待时间，防止频繁扩缩。监听 onChange 事件但不触发 rescale |
| **Idling** | 空闲期：冷却结束后，等待首次资源变化事件 |
| **Stabilizing** | 稳定期：检测到资源变化后，等待资源稳定下来。在此期间若达到 desired resources 则立即触发；否则等到超时后进入 Stabilized |
| **Stabilized** | 稳定完成：触发 onTrigger 时若有足够资源（sufficient）则 rescale，否则回到 Idling |
| **Transitioning** | 终态：已触发 rescale，不再接受事件 |

### 3.2 两种资源判断标准

- **hasSufficientResources()** — 可用 Slot 数量满足作业最低运行要求（可以降低并行度运行）
- **hasDesiredResources()** — 可用 Slot 数量满足作业完整并行度要求（理想状态）

在 `Executing` 状态中，还增加了一个关键判断：**parallelismChanged()** — 新的可用并行度与当前运行并行度**是否发生了变化**。如果可用资源不变，即使"充足"也不会触发无意义的 rescale。

### 3.3 Rescale 触发条件

Rescale 可以由以下事件触发：

1. **新 Slot 可用** — `onNewResourcesAvailable()` → `stateTransitionManager.onChange()`
2. **资源需求变更**（通过 REST API）— `onNewResourceRequirements()` → `stateTransitionManager.onChange()`
3. **Checkpoint 完成** — `onCompletedCheckpoint()` → `stateTransitionManager.onTrigger()`
4. **连续 N 次 Checkpoint 失败** — `onFailedCheckpoint()` → 当失败次数达到阈值后 `stateTransitionManager.onTrigger()`
5. **故障恢复** — 通过 `Restarting` 状态重新分配资源

Checkpoint 触发机制是为了确保在 rescale 时有一个一致的检查点可以恢复。配置项 `SCHEDULER_RESCALE_TRIGGER_MAX_CHECKPOINT_FAILURES` 控制连续失败几次后强制触发。

### 3.4 Rescale 执行流程

```
Executing (检测到资源变化)
    │
    │ transitionToSubsequentState()
    ▼
Restarting (取消当前 ExecutionGraph，等待所有 Task 终止)
    │
    │ onGloballyTerminalState(CANCELED)
    │
    ├── 如果并行度未变化 且资源与 restartWithParallelism 一致
    │   → CreatingExecutionGraph (直接用新并行度创建)
    │
    └── 否则
        → WaitingForResources (重新等待资源)
            │
            ▼
        CreatingExecutionGraph
            │
            │ 1. determineParallelism() — 计算新并行度
            │ 2. 调整 JobGraph 中各顶点的并行度
            │ 3. 创建新 ExecutionGraph + 恢复 Checkpoint 状态
            │ 4. tryToAssignSlots() — 分配 Slot
            ▼
        Executing (新并行度运行)
```

---

## 4. Slot 分配与状态局部性

### 4.1 SlotAllocator 接口

`SlotAllocator` 是资源分配的核心抽象，提供 3 个关键方法：

```java
// 计算所需 Slot 总数
ResourceCounter calculateRequiredSlots(Iterable<VertexInformation> vertices);

// 根据可用 Slot 确定各顶点可运行的并行度
Optional<VertexParallelism> determineParallelism(JobInformation, Collection<SlotInfo>);

// 尝试预留 Slot
Optional<ReservedSlots> tryReserveResources(JobSchedulingPlan);
```

### 4.2 SlotSharingSlotAllocator（默认实现）

支持 **Slot 共享**的分配器，考虑以下因素：
- **Slot Sharing Group** — 同一组的 Task 可以共享一个 Slot
- **任务均衡** — `TasksBalancedSlotMatchingResolver` / `SlotsBalancedSlotMatchingResolver`
- **最小 TaskManager 优先** — `minimalTaskManagerPreferred` 配置
- **负载均衡** — `TaskManagerLoadBalanceMode`

### 4.3 StateLocalitySlotAssigner（状态局部性优化）

rescale 时的关键优化：尽量将有状态算子分配到与原始 Slot 相同的 TaskManager 上，减少状态迁移量：

1. 从最新的 `CompletedCheckpoint` 获取各子任务的 keyed state 大小（`StateSizeEstimates`）
2. 计算新旧 KeyGroupRange 的**交集**，估算如果分配到同一 Slot 能保留多少本地状态
3. 使用**优先队列**按得分降序匹配 Slot，最大化状态局部性

```java
// 状态大小估算逻辑
private static long estimateSize(KeyGroupRange newRange, VertexAllocationInformation allocation) {
    int numberOfKeyGroups = oldRange.getIntersection(newRange).getNumberOfKeyGroups();
    long keyGroupSize = stateSizeInBytes / oldRange.getNumberOfKeyGroups();
    return numberOfKeyGroups * keyGroupSize;  // 估算可保留的本地状态量
}
```

---

## 5. DeclarativeSlotPool（声明式资源池）

`DeclarativeSlotPool` 是 Adaptive Scheduler 配套的资源管理方式，替代传统的命令式 Slot 请求：

```java
public interface DeclarativeSlotPool {
    void setResourceRequirements(ResourceCounter resourceRequirements);  // 声明所需资源
    void increaseResourceRequirementsBy(ResourceCounter increment);      // 增量声明
    void decreaseResourceRequirementsBy(ResourceCounter decrement);      // 减量声明
    Collection<ResourceRequirement> getResourceRequirements();           // 当前需求
    FreeSlotTracker getFreeSlotTracker();                                // 空闲 Slot 追踪
    void releaseIdleSlots(long currentTimeMillis);                       // 释放空闲 Slot
    void registerNewSlotsListener(NewSlotsListener listener);           // 监听新 Slot
}
```

ResourceManager 收到声明后，根据集群当前资源情况尽力提供 Slot。当 TaskManager 新注册或者 Slot 释放时，DeclarativeSlotPool 通知 AdaptiveScheduler，触发资源重评估。

---

## 6. Reactive Mode（被动模式）

Reactive Mode 是 Adaptive Scheduler 的一种**特殊执行模式**，通过配置 `scheduler-mode: REACTIVE` 开启。

### 6.1 核心区别

| 特性 | 普通 Adaptive Mode | Reactive Mode |
|------|-------------------|---------------|
| 并行度上限 | 用户设置的 parallelism | **maxParallelism**（尽可能使用所有资源）|
| 资源等待超时 | 有超时（默认 5 分钟）| 永不超时（无限等待）|
| 资源稳定超时 | 默认 10 秒 | 0 秒（立即开始）|
| REST API 修改并行度 | 支持 | **不支持**（抛出 UnsupportedOperationException）|
| 部署模式 | 所有 | 仅 **standalone application** 部署 |

### 6.2 实现方式

```java
// AdaptiveScheduler.computeVertexParallelismStore()
if (executionMode == SchedulerExecutionMode.REACTIVE) {
    return computeReactiveModeVertexParallelismStore(
            jobGraph.getVertices(), SchedulerBase::getDefaultMaxParallelism, true);
}
```

在 Reactive Mode 中：
- 每个顶点的 parallelism 被设置为 maxParallelism
- 声明的资源需求是"最大可能"
- ResourceManager 提供多少 Slot，作业就使用多少
- 外部自动扩缩容系统（如 Kubernetes HPA）可以通过增加/减少 TaskManager 来控制并行度

---

## 7. Adaptive Batch Scheduler（自适应批调度器）

`AdaptiveBatchScheduler` 是专门为**批处理**作业设计的自适应调度器，核心目标不同于流处理的 Adaptive Scheduler：

### 7.1 设计理念

- **按阶段调度**：利用批处理的 blocking 数据交换特性，上游执行完成后才调度下游
- **根据数据量决定并行度**：根据上游产出的实际数据量，自动决定下游算子的最佳并行度
- **Speculative Execution**：支持推测执行（`SpeculativeExecutionHandler`）

### 7.2 核心组件

| 类 | 职责 |
|----|------|
| `AdaptiveBatchScheduler` | 继承 `DefaultScheduler`，在上游完成后动态设置下游并行度 |
| `VertexParallelismAndInputInfosDecider` | 并行度决策器：根据数据量、配置的目标数据量等计算并行度 |
| `BlockingResultInfo` | 记录 blocking 结果集的数据量信息 |
| `AdaptiveExecutionHandler` | 支持运行时修改 StreamGraph |
| `StreamGraphOptimizer` | StreamGraph 优化策略 |
| `BatchJobRecoveryHandler` | 批作业恢复处理 |

### 7.3 工作流程

```
Source（已知并行度）
    │
    │ 执行完成，产出 blocking 数据
    │ 统计各 partition 的数据量
    ▼
VertexParallelismAndInputInfosDecider
    │
    │ 根据数据量 / target-data-volume 计算下游并行度
    │ 考虑 min/max parallelism 约束
    ▼
下游算子（动态设定的并行度）
    │
    │ 执行完成...
    ▼
继续调度下一层...
```

---

## 8. REST API 支持

Flink 提供 REST API 允许在运行时修改作业的资源需求：

### 8.1 查询当前资源需求

```
GET /jobs/:jobid/resource-requirements
```

返回各 JobVertex 的当前 min/max parallelism。

### 8.2 更新资源需求

```
PUT /jobs/:jobid/resource-requirements
```

允许用户动态修改各 JobVertex 的并行度范围。AdaptiveScheduler 收到后：

```java
// AdaptiveScheduler.updateJobResourceRequirements()
void updateJobResourceRequirements(JobResourceRequirements jobResourceRequirements) {
    // 1. 更新并行度存储
    this.jobInformation = new JobGraphJobInformation(jobGraph, updatedStore);
    // 2. 重新声明资源需求
    declareDesiredResources();
    // 3. 通知当前状态资源需求变化
    state.tryRun(ResourceListener.class, ResourceListener::onNewResourceRequirements, ...);
}
```

**注意**：Reactive Mode 下不支持此 API。

---

## 9. Rescale Timeline（扩缩容时间线追踪 — FLIP-495）

这是一个较新的功能，用于详细记录每次 rescale 的完整过程，便于运维排查：

```
Rescale 记录包含：
├── RescaleIdInfo (rescale UUID, 资源需求 ID, 尝试 ID)
├── TriggerCause (触发原因：初始调度/资源变化/需求更新/故障恢复)
├── 各顶点的并行度变化：
│   ├── desired parallelism (期望并行度)
│   ├── sufficient parallelism (最低并行度)
│   ├── pre-rescale parallelism (扩缩前)
│   └── post-rescale parallelism (扩缩后)
├── 各 SlotSharingGroup 的 Slot 变化：
│   ├── desired/minimal required/pre/post rescale slots
│   └── acquired resource profile
├── 调度器状态变迁时间线：
│   └── [state, enter_ts, leave_ts, duration, exception]
├── 开始/结束时间戳
├── 终态 (SUCCEED/FAILED)
└── 终止原因
```

---

## 10. 配置参数一览

### 10.1 Adaptive Scheduler 核心配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `scheduler-mode` | null | 设为 `REACTIVE` 开启被动模式 |
| `jobmanager.adaptive-scheduler.submission.resource-wait-timeout` | 5 min | 提交后等待资源的最大超时 |
| `jobmanager.adaptive-scheduler.submission.resource-stabilization-timeout` | 10 s | 提交时资源稳定等待时间 |
| `jobmanager.adaptive-scheduler.executing.cooldown-after-rescaling` | 30 s | 两次 rescale 之间的最小冷却时间 |
| `jobmanager.adaptive-scheduler.executing.resource-stabilization-timeout` | 60 s | 执行时资源稳定等待时间 |
| `jobmanager.adaptive-scheduler.rescale-trigger.max-checkpoint-failures` | 2 | 连续多少次 CP 失败后强制触发 rescale |
| `jobmanager.adaptive-scheduler.rescale-trigger.max-delay` | 自动计算 | rescale 触发的最大延迟 |
| `jobmanager.adaptive-scheduler.prefer-minimal-taskmanagers` | false | 是否优先使用最少 TaskManager |

### 10.2 Adaptive Batch Scheduler 配置

| 配置项 | 说明 |
|--------|------|
| `execution.batch.adaptive.auto-parallelism.enabled` | 是否启用自适应并行度 |
| `execution.batch.adaptive.auto-parallelism.min-parallelism` | 自适应并行度最小值 |
| `execution.batch.adaptive.auto-parallelism.max-parallelism` | 自适应并行度最大值 |
| `execution.batch.adaptive.auto-parallelism.avg-data-volume-per-task` | 每个 Task 处理的目标数据量 |
| `execution.batch.speculative.enabled` | 是否启用推测执行 |

---

## 11. 整体架构图

```
                                用户/外部系统
                                     │
                         ┌───────────┴───────────┐
                         │    REST API            │
                         │  (resource-requirements│
                         │   查询/更新)            │
                         └───────────┬───────────┘
                                     │
                         ┌───────────▼───────────┐
                         │   AdaptiveScheduler    │
                         │  (有限状态机)           │
                         │                        │
                         │  Created → Waiting →   │
                         │  Creating → Executing  │
                         │  → Restarting → ...    │
                         └──┬────────────────┬───┘
                            │                │
              ┌─────────────▼──┐    ┌───────▼──────────┐
              │ DeclarativeSlot│    │ StateTransition   │
              │ Pool           │    │ Manager           │
              │ (声明式资源)    │    │ (Cooldown→Idling  │
              │                │    │  →Stabilizing→    │
              │ setResource    │    │  Transitioning)   │
              │ Requirements() │    └──────────────────┘
              └──┬─────────────┘
                 │ Slot变化回调
    ┌────────────▼────────────┐
    │    ResourceManager      │
    │  (资源调度)              │
    │                         │
    │  TaskManager 注册/退出   │
    └─────────────────────────┘
                 │
    ┌────────────▼────────────┐
    │  SlotSharingSlotAllocator│
    │  (Slot 分配)             │
    │                         │
    │  ├── determineParallelism│
    │  ├── tryReserveResources │
    │  └── StateLocality      │
    │      SlotAssigner        │
    │      (状态局部性优化)     │
    └─────────────────────────┘
```

---

## 12. 与外部自动扩缩容系统的配合

### 12.1 Kubernetes Autoscaler（flink-kubernetes-operator）

Flink 的 Kubernetes Operator 项目 (`flink-kubernetes-operator`) 提供了一个独立的 Autoscaler 模块，它：

1. 收集 Flink Job 的 metrics（吞吐量、背压、延迟）
2. 计算各算子的"真实"需要的并行度
3. 通过 REST API (`PUT /jobs/:jobid/resource-requirements`) 通知 AdaptiveScheduler 调整并行度
4. AdaptiveScheduler 内部执行 rescale

### 12.2 Reactive Mode + Kubernetes HPA

另一种方式是使用 Reactive Mode：

1. Flink 作业以 Reactive Mode 部署
2. 配置 Kubernetes HPA 监控 CPU/内存/自定义 metrics
3. HPA 增加/减少 TaskManager Pod 数量
4. AdaptiveScheduler 自动感知 Slot 变化，调整并行度到最大可用

---

## 13. 设计亮点与局限性

### 13.1 设计亮点

1. **精巧的状态机设计**：外层 8 个状态 + 内层 5 个 Phase，职责清晰、转换规则明确
2. **声明式资源管理**：解耦了资源请求和资源分配，简化了扩缩容逻辑
3. **状态局部性优化**：通过 KeyGroup 范围交集估算，最小化 rescale 时的状态迁移
4. **Checkpoint 感知触发**：在有一致检查点时触发 rescale，保证数据一致性
5. **冷却和稳定机制**：多重时间窗口防止频繁无效扩缩
6. **Rescale Timeline**：完整的扩缩容过程追踪，便于运维

### 13.2 当前局限性

1. **仅支持流处理**（Adaptive Scheduler）：`assertPreconditions` 中明确检查 `JobType.STREAMING`
2. **仅支持 pipelined 数据交换**：不支持 blocking result partition
3. **Rescale = 全量重启**：通过取消当前执行、创建新 ExecutionGraph、从 Checkpoint 恢复实现。不是真正的"在线扩缩"（无需重启）
4. **Reactive Mode 仅支持 standalone**：不支持 YARN/Kubernetes active resource manager
5. **不自主决策最优并行度**：Adaptive Scheduler 本身不包含负载分析能力，需要外部系统（如 K8s Operator Autoscaler）来做智能决策

---

## 14. 总结

Flink 的自动扩缩容是一个**分层协作**的体系：

- **底层**：`DeclarativeSlotPool` 提供声明式资源管理，`SlotAllocator` 提供智能 Slot 分配
- **中层**：`AdaptiveScheduler` 通过精巧的状态机管理作业生命周期，`DefaultStateTransitionManager` 控制 rescale 时机
- **上层**：REST API 暴露资源需求接口，外部系统（K8s Operator Autoscaler、HPA 等）提供智能决策

这种设计将**机制**（如何 rescale）与**策略**（何时/如何决定新并行度）分离，使得 Flink 既可以独立工作（Reactive Mode），也可以与外部智能系统深度集成。
