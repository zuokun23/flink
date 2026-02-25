# Flink 调度系统深度架构分析

> 基于 Flink 源码的调度组件拆解，并与 Spark、Ray 等计算引擎对比，解释 Flink 调度为何如此设计。

---

## 一、高屋建瓴：三大引擎的本质差异决定调度设计

在分析 Flink 的调度之前，我们先厘清一个根本问题：**计算引擎的调度系统是为其核心计算模型服务的**。Spark、Ray 和 Flink 之所以调度设计迥异，根源在于它们要解决的问题不同。

| 维度 | Spark | Ray | Flink |
|------|-------|-----|-------|
| **核心模型** | 批处理(BSP模型) | 通用分布式计算(Actor/Task) | 流批一体(Dataflow模型) |
| **数据交换** | Shuffle落盘，Stage间阻塞 | 对象存储/共享内存 | 流式Pipeline + 批式Blocking |
| **时间语义** | 无 | 无 | 事件时间/处理时间/水位线 |
| **状态管理** | 无内置状态 | Actor内状态 | 一等公民(Checkpoint/Savepoint) |
| **生命周期** | Job级别，运行完即销毁 | 长期运行的集群 | 长期运行的流作业 |
| **容错粒度** | Stage级别重算 | Task级别重启 | Region级别精准恢复 |

### 1.1 Spark 的调度：为"批"而生

Spark 采用经典的 **BSP (Bulk Synchronous Parallel)** 模型：

```
Stage 1 (Map)       Stage 2 (Reduce)
  Task 0  ──┐
  Task 1  ──┼── Shuffle 落盘 ──┬── Task 0
  Task 2  ──┘                  └── Task 1
```

- **DAGScheduler** 把 Job 切分为多个 Stage，Stage 之间通过 Shuffle 解耦
- **TaskScheduler** 负责每个 Stage 内 Task 的调度
- 上游 Stage 所有 Task 完成后，下游 Stage 才能开始
- 容错简单粗暴：某个 Task 失败就重算该 Task（数据在 Shuffle 文件里还在），Stage 失败就重跑整个 Stage

**为什么 Spark 可以这样设计？** 因为批处理天然有"终止"特性，每个 Stage 的结果会物化(materialize)到磁盘。这种设计简单、易于理解，但**无法处理无界数据流**。

### 1.2 Ray 的调度：为"通用性"而生

Ray 的调度更接近一个**分布式操作系统**：

- **GCS (Global Control Store)** 维护全局元数据
- **Raylet** 在每个节点上做本地调度
- Task 和 Actor 是一等公民，调度就是"把函数放到有资源的地方执行"
- 采用去中心化的**二级调度**：先本地调度，本地资源不够再溢出到远端

**Ray 不关心数据流拓扑**，它的调度粒度是单个函数调用或 Actor。这使得 Ray 非常灵活，可以做强化学习、分布式训练等，但在**流处理场景下缺乏精细的背压、状态管理和一致性保证**。

### 1.3 Flink 的调度：为"流"而生，兼顾"批"

Flink 的调度系统面临的核心挑战是：

1. **长期运行的 Pipeline**：算子之间通过网络直接传输数据，上下游同时在线
2. **精确一次的状态一致性**：Checkpoint 需要与调度深度集成
3. **流批一体**：同一套调度体系需要同时支持 Pipeline（流）和 Blocking（批）数据交换
4. **动态资源适配**：集群资源变化时作业能自适应调整

**这就是为什么 Flink 不能照搬 Spark 的 Stage 模型，也不能用 Ray 的通用 Task 模型。Flink 需要一个理解数据流拓扑、能精准做故障恢复、且与 Checkpoint 深度绑定的调度系统。**

---

## 二、Flink 调度系统的整体架构

从源码中可以看到，Flink 的调度系统由以下层次组成：

```
┌─────────────────────────────────────────────────────┐
│                    JobMaster                         │
│  (作业生命周期管理，RPC入口)                           │
├─────────────────────────────────────────────────────┤
│               SchedulerNG (调度器接口)                │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  │
│  │SchedulerBase│  │AdaptiveScheduler│ │AdaptiveBatch│ │
│  │(默认调度基类)│  │(自适应流调度器)  │ │Scheduler    │ │
│  └──────┬──────┘  └──────────────┘  └────────────┘  │
│         │                                           │
│  ┌──────▼──────────────────────────────────────┐    │
│  │         DefaultScheduler (默认调度器)         │    │
│  │  ┌──────────────┐ ┌───────────────────────┐ │    │
│  │  │SchedulingStrategy│ FailoverStrategy    │ │    │
│  │  │(调度策略)        │ (故障恢复策略)        │ │    │
│  │  └──────────────┘ └───────────────────────┘ │    │
│  │  ┌──────────────────────────────────────┐   │    │
│  │  │    ExecutionSlotAllocator             │   │    │
│  │  │    (执行槽位分配器)                    │   │    │
│  │  └──────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────┤
│             ExecutionGraph (执行图)                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │ExecutionJob  │  │Execution     │  │Intermediate  │ │
│  │Vertex        │  │Vertex        │  │Result        │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  │
├─────────────────────────────────────────────────────┤
│             SlotPool (槽池)                          │
│  ┌──────────────────┐  ┌──────────────────────┐     │
│  │DeclarativeSlotPool│  │PhysicalSlotProvider  │     │
│  └──────────────────┘  └──────────────────────┘     │
├─────────────────────────────────────────────────────┤
│          ResourceManager + SlotManager              │
│          (资源管理器 + 槽位管理器)                    │
├─────────────────────────────────────────────────────┤
│          TaskManager (任务执行器)                     │
│          (实际执行Task的进程)                         │
└─────────────────────────────────────────────────────┘
```

---

## 三、核心组件逐层拆解

### 3.1 SchedulerNG：调度器的顶层接口

```
SchedulerNG (接口)
  ├── startScheduling()          // 启动调度
  ├── cancel()                   // 取消作业
  ├── updateTaskExecutionState() // 处理Task状态变更
  ├── triggerSavepoint()         // 触发Savepoint
  ├── triggerCheckpoint()        // 触发Checkpoint
  ├── acknowledgeCheckpoint()    // 确认Checkpoint
  └── handleGlobalFailure()      // 处理全局失败
```

从源码 `SchedulerNG.java` 可以看到，**调度器接口直接包含了 Checkpoint/Savepoint 相关方法**。这是 Flink 调度与 Spark/Ray 最本质的区别之一——**调度和状态一致性是一体设计的，不可分割**。

在 Spark 中，容错由 RDD lineage 和 Shuffle 文件保证，调度器不需要关心状态。在 Ray 中，容错靠对象重建。但在 Flink 中，因为流作业是长期运行的，Checkpoint 是唯一的一致性恢复手段，调度器必须在恢复时协调 Checkpoint 的恢复。

**为什么不分离？** 源码中的注释揭示了原因：

> *"These are necessary as long as the Operator Coordinators are part of the scheduler. There are good reasons to pull them out of the Scheduler and make them directly a part of the JobMaster. However, we would need to rework the complete CheckpointCoordinator initialization before we can do that."*

这说明当前设计有一定的历史包袱，但核心思想是合理的：调度恢复时必须同步恢复 Checkpoint 状态。

### 3.2 SchedulerBase：调度器的骨架

`SchedulerBase` 是所有调度器实现的抽象基类，它承载了以下核心职责：

**1) ExecutionGraph 的创建和管理**

```java
// SchedulerBase 构造函数中
this.executionGraph = createAndRestoreExecutionGraph(...);
this.schedulingTopology = executionGraph.getSchedulingTopology();
```

ExecutionGraph 是 JobGraph 的运行时表示，它追踪每个 Task 的执行状态。这类似于一个"活的"DAG——节点状态在不断变化。

**2) 状态恢复的协调**

```java
protected void restoreState(Set<ExecutionVertexID> vertices, boolean isGlobalRecovery) {
    // 恢复 EndOfData 标记
    vertexEndOfDataListener.restoreVertices(vertices);
    // 获取 CheckpointCoordinator
    CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
    // 中止未完成的 Checkpoint
    checkpointCoordinator.abortPendingCheckpoints(...);
    // 恢复到最近的 Checkpoint
    if (isGlobalRecovery) {
        checkpointCoordinator.restoreLatestCheckpointedStateToAll(...);
    } else {
        checkpointCoordinator.restoreLatestCheckpointedStateToSubtasks(...);
    }
}
```

这段代码体现了 Flink 的精髓：**失败恢复不是简单地重启 Task，而是要恢复到一致的 Checkpoint 状态**。全局恢复和局部恢复走不同路径，局部恢复只恢复受影响子任务的状态。

**3) 模板方法模式**

SchedulerBase 使用模板方法模式，将具体决策留给子类：

```java
protected abstract void startSchedulingInternal();       // 如何启动调度
protected abstract void onTaskFinished(Execution, IOMetrics); // Task完成后做什么
protected abstract void onTaskFailed(Execution);         // Task失败后做什么
protected abstract void cancelAllPendingSlotRequestsInternal(); // 如何取消Slot请求
```

### 3.3 DefaultScheduler：标准调度器

DefaultScheduler 是最常用的调度器实现，它组合了三个核心策略：

#### 3.3.1 SchedulingStrategy（调度策略）—— 决定"何时调度哪些Task"

```
SchedulingStrategy (接口)
  ├── startScheduling()      // 初始调度
  ├── restartTasks()         // 重启Task
  ├── onExecutionStateChange() // 响应状态变更
  └── onPartitionConsumable()  // 响应分区可消费
```

**两种实现的设计哲学：**

**PipelinedRegionSchedulingStrategy（流水线区域调度）**

这是流处理的默认策略。核心概念是"Pipeline Region"——通过 Pipeline 数据交换连接的一组 Task 必须同时运行。

```
Source ──Pipeline──> Map ──Pipeline──> Sink
       ↑ 这三个构成一个 PipelinedRegion  ↑
```

为什么要一起调度？因为 Pipeline 交换是阻塞的——如果下游没有运行，上游的输出缓冲区会满，最终导致背压停滞。

从源码来看，它的调度逻辑是：
1. 找到所有"源头"Region（没有外部 Blocking 输入的 Region）
2. 调度源头 Region 内的所有 Task
3. 当某个 Region 的所有输出分区都完成后（对于 Blocking 交换），调度下游 Region

**VertexwiseSchedulingStrategy（逐顶点调度）**

这是批处理的调度策略。它以单个 Task 为粒度进行调度，一个 Task 只要输入数据就绪就可以调度。

**两种策略的对比，恰好体现了流与批的本质差异：**
- 流处理：上下游必须同时在线 → Region 级调度
- 批处理：上游完成后数据已物化 → 逐 Task 调度

**对比 Spark：** Spark 只有一种调度逻辑——Stage by Stage。这在纯批场景下够用，但无法支持流处理。Flink 通过策略模式支持两种调度方式，这就是"流批一体"在调度层的体现。

#### 3.3.2 FailoverStrategy（故障恢复策略）—— 决定"Task失败时重启谁"

```
FailoverStrategy (接口)
  └── getTasksNeedingRestart(ExecutionVertexID, Throwable) → Set<ExecutionVertexID>
```

两种实现形成鲜明对比：

**RestartAllFailoverStrategy（全部重启）**

```java
public Set<ExecutionVertexID> getTasksNeedingRestart(...) {
    return topology.getVertices().stream()
            .map(SchedulingExecutionVertex::getId)
            .collect(Collectors.toSet());
}
```

一个 Task 失败，重启全部。简单但代价高。

**RestartPipelinedRegionFailoverStrategy（Region级精准恢复）**

这是 Flink 调度的精华之一。它的恢复逻辑：

1. 找到失败 Task 所在的 PipelinedRegion
2. 该 Region 必须重启
3. **如果该 Region 的输入数据不可用**（比如上游 Blocking 分区的 Task 已经完成但数据丢失了），则上游 Region 也要重启
4. **被重启 Region 的所有下游消费者 Region 也要重启**（因为数据一致性）

```
Region A ──Blocking──> Region B ──Pipeline──> Region C
                           ↑ 失败
```

如果 Region B 失败：
- Region B 必须重启
- 如果 Region A 的输出数据还在 → 只重启 B 和 C
- 如果 Region A 的输出数据丢了 → A、B、C 都要重启

**对比 Spark：** Spark 的 Stage 失败恢复逻辑相似（重算丢失的 Shuffle 数据），但 Spark 不需要考虑"同时在线的 Pipeline 连接"。Flink 的 Region 概念是 Pipeline 语义的直接产物。

**对比 Ray：** Ray 的故障恢复是 Task/Actor 级别的，不理解数据流拓扑，无法做这种精准的区域恢复。

#### 3.3.3 RestartBackoffTimeStrategy（重启回退策略）—— 决定"何时重启、是否放弃"

```
RestartBackoffTimeStrategy
  ├── FixedDelayRestartBackoffTimeStrategy     // 固定延迟，有限次数
  ├── ExponentialDelayRestartBackoffTimeStrategy // 指数退避
  ├── FailureRateRestartBackoffTimeStrategy    // 失败率限制
  └── NoRestartBackoffTimeStrategy             // 不重启
```

这在 Spark 中有类似对应（`spark.task.maxFailures`），但 Flink 的版本更精细，支持指数退避等高级策略。

#### 3.3.4 ExecutionSlotAllocator（Slot分配器）—— 决定"Task部署到哪个Slot"

```
ExecutionSlotAllocator
  ├── SimpleExecutionSlotAllocator       // 简单分配
  └── SlotSharingExecutionSlotAllocator  // 支持Slot共享
```

**Slot共享** 是 Flink 独特的设计：同一个 SlotSharingGroup 中的不同算子的子任务可以共享一个 Slot。

```
┌──────────── Slot ────────────┐
│  Source[0]  Map[0]  Sink[0]  │  ← 一条完整的 Pipeline 链共享一个 Slot
└──────────────────────────────┘
```

**好处：**
- 减少网络传输（同 Slot 内的算子可以线程间传输）
- 简化资源配置（用户只需指定整体并行度，不用为每个算子单独配资源）
- 提高资源利用率

**对比 Spark/Ray：** Spark 的 Task 独占 Core，没有 Slot 共享概念。Ray 的 Task 独占资源，Actor 可以多路复用。Flink 的 Slot 共享是流处理 Pipeline 模型的最优资源分配方式。

### 3.4 AdaptiveScheduler：自适应调度器

AdaptiveScheduler 是 Flink 的高级调度器（FLIP-160），它引入了**状态机模式**来管理调度生命周期：

```
Created → WaitingForResources → CreatingExecutionGraph → Executing
                    ↑                                        │
                    └──── Restarting ←── Failing ←──────────┘
                                                             │
                                                         Finished
```

**每个状态都是一个独立的类**，这种设计使得：
1. 各状态的行为清晰隔离
2. 不该在某状态执行的操作会被自动拒绝
3. 状态转换有明确的规则

**核心特性：声明式资源管理**

AdaptiveScheduler 使用 `DeclarativeSlotPool`，这是与 DefaultScheduler 最大的区别：

```java
// 声明式：我需要这么多资源
slotPool.setResourceRequirements(resourceRequirements);
// vs 命令式（DefaultScheduler）：给我一个 Slot
slotPool.requestSlot(slotRequestId, resourceProfile);
```

**声明式的好处：**
- 资源增减时自动适应并行度
- 不需要精确地一对一分配，ResourceManager 可以灵活调度
- 支持 Reactive 模式——有多少资源就用多少

**为什么 Spark 不需要这个？** 批处理有明确的结束时间，资源不够就排队等。流处理是无限运行的，资源波动时必须自适应。

### 3.5 AdaptiveBatchScheduler：自适应批调度器

`AdaptiveBatchScheduler` 继承自 `DefaultScheduler`，专门为批处理优化。它的核心能力是**运行时动态决定并行度**：

上游 Stage 完成后，根据输出数据量动态确定下游的并行度。这类似于 Spark 的 Adaptive Query Execution (AQE)，但 Flink 的实现更原生地嵌入调度框架。

### 3.6 ExecutionGraph：调度的核心数据结构

```
JobGraph (逻辑图)                    ExecutionGraph (物理执行图)
┌────────┐                          ┌─────────────────────────┐
│JobVertex│  ──(展开并行度)──>       │ExecutionJobVertex       │
│(Map, p=3)│                        │ ├── ExecutionVertex[0]   │
└────────┘                          │ │    └── Execution (尝试)│
                                    │ ├── ExecutionVertex[1]   │
                                    │ │    └── Execution       │
                                    │ └── ExecutionVertex[2]   │
                                    │      └── Execution       │
                                    └─────────────────────────┘
```

三层结构：
- **ExecutionJobVertex**：对应一个 JobVertex（逻辑算子），包含并行度信息
- **ExecutionVertex**：一个并行子任务，有具体的部署信息
- **Execution**：一次执行尝试，失败后会创建新的 Execution

**SchedulingTopology** 是 ExecutionGraph 的调度视图，提供 PipelinedRegion 的划分。

### 3.7 资源管理：SlotPool + SlotManager

```
JobMaster                         ResourceManager
┌──────────────────────┐         ┌──────────────────────┐
│     SlotPool          │ ──────→│     SlotManager       │
│ (每个Job一个)         │ 请求资源 │ (全局唯一)            │
│                      │         │                      │
│ 管理已分配给该Job的    │←──────  │ 管理所有TaskManager    │
│ Slot                  │分配Slot │ 的所有Slot             │
└──────────────────────┘         └──────────────────────┘
                                          │
                                    ┌─────▼─────┐
                                    │TaskManager │
                                    │  Slot 0    │
                                    │  Slot 1    │
                                    │  Slot 2    │
                                    └────────────┘
```

**二级资源管理**：
1. **SlotManager（全局级）**：在 ResourceManager 中，管理集群所有的 Slot，响应各 Job 的资源需求
2. **SlotPool（作业级）**：在 JobMaster 中，管理分配给当前 Job 的 Slot，提供给 Scheduler 使用

**对比 Spark：** Spark 由 Driver 直接向 Cluster Manager（YARN/K8s）申请资源，是一级模型。Flink 的二级模型能更好地隔离作业间的资源管理。

**对比 Ray：** Ray 也是二级调度（GCS + Raylet），但 Ray 的调度粒度更细（单个函数调用），Flink 的 Slot 是粗粒度的容器。

---

## 四、Flink 调度的设计哲学总结

### 4.1 "Pipeline-First"的调度理念

Flink 的所有调度决策都围绕 **PipelinedRegion** 这一核心概念展开：
- **调度单元**：PipelinedRegion（流）/ Vertex（批）
- **故障恢复单元**：PipelinedRegion
- **资源分配单元**：SlotSharingGroup（通常对应一个 Pipeline）

这与 Spark 的 "Stage-First" 和 Ray 的 "Task-First" 形成鲜明对比。

### 4.2 调度与 Checkpoint 深度绑定

Flink 在 `SchedulerBase` 中同时管理调度和 Checkpoint，这不是设计缺陷而是必要选择：
- 失败恢复时必须从 Checkpoint 恢复状态
- 局部恢复时只恢复受影响子任务的状态
- Savepoint 触发时需要协调所有运行中的 Task

### 4.3 策略模式实现可插拔

```
DefaultScheduler
  ├── SchedulingStrategy      (可插拔：Region vs Vertex)
  ├── FailoverStrategy        (可插拔：Region vs All)
  ├── RestartBackoffTimeStrategy (可插拔：Fixed vs Exponential vs FailureRate)
  └── ExecutionSlotAllocator  (可插拔：Simple vs SlotSharing)
```

通过策略模式，Flink 用一套调度框架同时支持流和批，而不是像 Spark 那样后来加流（Structured Streaming），或像 Ray 那样不原生支持流。

### 4.4 声明式 vs 命令式资源管理

Flink 同时支持两种模式：
- **命令式**（DefaultScheduler + SlotPool）：主动请求特定的 Slot
- **声明式**（AdaptiveScheduler + DeclarativeSlotPool）：声明需要的资源，由系统匹配

声明式是 Flink 的演进方向，它使得 Reactive 模式（根据可用资源自动缩放）成为可能。

---

## 五、设计优劣分析

### 优势

1. **精准的故障恢复**：Region 级恢复最小化重启范围，对长期运行的流作业至关重要
2. **流批一体**：同一框架通过策略模式支持两种执行模式
3. **状态一致性保证**：调度与 Checkpoint 的深度集成确保精确一次语义
4. **Slot 共享**：提高资源利用率，简化配置
5. **自适应能力**：AdaptiveScheduler 支持动态并行度调整

### 需要注意的复杂性

1. **调度器与 Checkpoint 的耦合**：增加了系统理解和维护的复杂度（源码注释也承认了这一点）
2. **多种调度器并存**：DefaultScheduler、AdaptiveScheduler、AdaptiveBatchScheduler 各有适用场景，增加了用户选择的负担
3. **资源管理的二级模型**：比 Spark 的一级模型更复杂，但带来了更好的隔离性

---

## 六、一张图总结

```
                        用户提交 JobGraph
                              │
                    ┌─────────▼─────────┐
                    │    JobMaster       │
                    │ (选择 Scheduler)    │
                    └─────────┬─────────┘
                              │
            ┌─────────────────┼─────────────────┐
            │                 │                 │
   ┌────────▼──────┐  ┌──────▼───────┐  ┌──────▼───────┐
   │DefaultScheduler│  │Adaptive      │  │AdaptiveBatch │
   │(标准调度)       │  │Scheduler     │  │Scheduler     │
   │                │  │(自适应流)     │  │(自适应批)     │
   └───────┬────────┘  └──────────────┘  └──────────────┘
           │
   ┌───────▼────────────────────────────┐
   │  SchedulingStrategy (何时调度)      │
   │  ├── PipelinedRegion (流: 整Region) │
   │  └── Vertexwise (批: 逐Task)       │
   │                                    │
   │  FailoverStrategy (失败恢复)        │
   │  ├── RestartPipelinedRegion (精准)  │
   │  └── RestartAll (全部重启)          │
   │                                    │
   │  ExecutionSlotAllocator (资源分配)  │
   │  └── SlotSharing (共享Slot)        │
   └───────┬────────────────────────────┘
           │
   ┌───────▼────────┐     ┌──────────────────┐
   │ ExecutionGraph  │     │  SlotPool         │
   │ (运行时物理图)   │────→│  (作业级Slot管理)  │
   └────────────────┘     └────────┬─────────┘
                                   │
                          ┌────────▼─────────┐
                          │  ResourceManager  │
                          │  + SlotManager    │
                          │  (集群级资源管理)   │
                          └────────┬─────────┘
                                   │
                          ┌────────▼─────────┐
                          │  TaskManager[]    │
                          │  (实际执行Task)    │
                          └──────────────────┘
```

**核心结论：Flink 调度系统的设计，是"流优先、兼顾批"思想在工程实现上的必然结果。它比 Spark 的调度更复杂，但这份复杂性恰恰是流处理语义所要求的——Pipeline 的同时在线、Region 级精准恢复、与 Checkpoint 的深度集成，三者缺一不可。**
