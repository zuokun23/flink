# Flink Table 模块深度分析

## 1. 整体概览

`flink-table` 是 Apache Flink 的关系型 API 核心模块，提供 **Table API** 和 **SQL** 两种统一的流批处理编程接口。该模块位于 `/workspace/flink-table/`，包含约 **4,992 个源文件**（Java + Scala），版本为 **2.3-SNAPSHOT**。

核心依赖：
- Apache Calcite 1.36.0（SQL 解析与优化框架）
- Janino 3.1.10（运行时代码编译）
- Guava 33.4.0-jre

---

## 2. 模块架构（19 个子模块）

```
flink-table/
├── 基础层（Common）
│   └── flink-table-common          # 类型系统、UDF、连接器 SPI、Catalog 接口
│
├── API 层
│   ├── flink-table-api-java        # Java Table API & SQL 入口
│   ├── flink-table-api-scala       # Scala Table API
│   ├── flink-table-api-bridge-base # DataStream 桥接基类
│   ├── flink-table-api-java-bridge # Java DataStream ↔ Table 桥接
│   ├── flink-table-api-scala-bridge# Scala DataStream ↔ Table 桥接
│   ├── flink-table-api-java-uber   # Uber JAR（供 flink-dist 使用）
│   └── flink-table-calcite-bridge  # Calcite API 桥接（用于 SQL 方言插件）
│
├── 解析与优化层
│   ├── flink-sql-parser            # ANSI SQL 解析器（基于 Calcite/JavaCC）
│   └── flink-table-planner         # 查询计划、优化、代码生成（核心）
│       ├── flink-table-planner-loader       # 类加载器隔离（Scala 版本隔离）
│       └── flink-table-planner-loader-bundle# 含 Scala 依赖的打包
│
├── 运行时层
│   ├── flink-table-runtime         # 算子实现、内建函数实现、类型转换
│   └── flink-table-code-splitter   # 生成代码的方法分割（64KB 限制）
│
├── SQL 客户端层
│   ├── flink-sql-client            # CLI 交互工具
│   ├── flink-sql-gateway-api       # SQL Gateway 接口定义
│   ├── flink-sql-gateway           # SQL Gateway 实现
│   ├── flink-sql-jdbc-driver       # JDBC 驱动
│   └── flink-sql-jdbc-driver-bundle# JDBC 驱动打包
│
└── 测试
    └── flink-table-test-utils      # 测试工具和 Harness
```

---

## 3. 核心架构设计

### 3.1 整体执行流程

```
SQL/Table API 查询
       │
       ▼
  ┌─────────────┐
  │   Parser     │  SQL 字符串 → Operation 树
  │ (Calcite +   │  (flink-sql-parser + flink-table-planner)
  │  JavaCC)     │
  └──────┬──────┘
         ▼
  ┌─────────────┐
  │  Operation   │  Table API 的中间表示（AST）
  │    Tree      │  (flink-table-api-java: operations/)
  └──────┬──────┘
         ▼
  ┌─────────────┐
  │  Planner     │  Operation → Calcite RelNode → 优化 → ExecNode
  │  (Calcite    │  (flink-table-planner: PlannerBase)
  │   Optimizer) │
  └──────┬──────┘
         ▼
  ┌─────────────┐
  │ Code         │  ExecNode → 生成 Java 代码 → 编译
  │ Generation   │  (codegen/ 包，Janino 编译)
  └──────┬──────┘
         ▼
  ┌─────────────┐
  │ Executor     │  Transformation → Pipeline → JobGraph → 提交集群
  │              │  (flink-table-api-java: delegation/Executor)
  └─────────────┘
```

### 3.2 关键接口与类

#### 3.2.1 TableEnvironment（入口）

`org.apache.flink.table.api.TableEnvironment` 是所有 Table/SQL 程序的入口：
- 连接外部系统
- 注册/检索 Table、Catalog
- 执行 SQL 语句
- 配置选项管理

实现类：`TableEnvironmentImpl`，内部组合了 `CatalogManager`、`FunctionCatalog`、`ModuleManager`、`Planner`、`Executor`。

#### 3.2.2 Table（核心抽象）

`org.apache.flink.table.api.Table` 描述一个数据转换管道（类似 SQL 中的视图），支持：
- `select()`, `filter()`, `where()`, `join()`, `groupBy()`, `orderBy()` 等关系操作
- `execute()` 本地执行
- `executeInsert()` 写入 Sink
- 每次转换生成新的 Table 对象（不可变）

实现类：`TableImpl`，内部持有 `QueryOperation` 树。

#### 3.2.3 Planner（三件套委托模式）

`delegation/` 包定义了三个核心接口，将 API 层与具体实现解耦：

| 接口 | 职责 | 实现 |
|------|------|------|
| `Parser` | SQL 字符串 → Operation 树 | `ParserImpl`（封装 Calcite FlinkPlannerImpl） |
| `Planner` | Operation → Transformation（计划+优化+翻译） | `PlannerBase`（BatchPlanner / StreamPlanner） |
| `Executor` | Transformation → Pipeline → 集群执行 | `DefaultExecutor` |

#### 3.2.4 Connector SPI（Source/Sink）

```
DynamicTableSource（接口）
├── ScanTableSource    # 全表扫描/changelog 读取
└── LookupTableSource  # 点查（lookup join）

DynamicTableSink（接口）
└── 支持 append / retract / upsert 模式

DynamicTableSourceFactory / DynamicTableSinkFactory
└── 通过 SPI 发现和创建 Source/Sink 实例
```

Source/Sink 通过 **Abilities 接口**声明能力，Planner 据此进行优化下推：
- `SupportsFilterPushDown` — 谓词下推
- `SupportsProjectionPushDown` — 列裁剪
- `SupportsPartitionPushDown` — 分区裁剪
- `SupportsLimitPushDown` — Limit 下推
- `SupportsWatermarkPushDown` — Watermark 下推
- `SupportsDynamicFiltering` — 动态过滤
- `SupportsAggregatePushDown` — 聚合下推
- `SupportsStatisticReport` — 统计信息上报

---

## 4. 关键子模块深度分析

### 4.1 flink-table-common（基础层）

**约 748 个 Java 文件**，提供所有上层模块依赖的核心定义：

| 包 | 内容 |
|---|------|
| `types/` | **类型系统**：182 个文件，定义 LogicalType 体系（45 种逻辑类型）、DataType、类型推导（inference/）、类型提取（extraction/） |
| `data/` | **内部数据结构**：RowData、StringData、ArrayData、MapData 等二进制格式 |
| `connector/` | **连接器 SPI**：DynamicTableSource/Sink、Format、Abilities 接口 |
| `catalog/` | **Catalog 接口**：Catalog、CatalogTable、CatalogView、ResolvedSchema、CatalogStore |
| `functions/` | **UDF 框架**：ScalarFunction、TableFunction、AggregateFunction、ProcessTableFunction |
| `expressions/` | **表达式体系**：Expression、CallExpression、FieldReferenceExpression |
| `factories/` | **工厂 SPI**：Factory、DynamicTableFactory、FactoryUtil |

**类型系统层次：**
```
LogicalType（逻辑类型，如 VARCHAR、INT、ROW）
    ↓ 包装
DataType（逻辑类型 + 物理转换类）
    ↓ 序列化/反序列化
RowData（内部二进制行格式）
```

### 4.2 flink-table-api-java（API 层）

**约 389 个 Java 文件**，核心包：

| 包 | 内容 |
|---|------|
| `api/` | 用户 API：Table、TableEnvironment、窗口定义（Tumble/Slide/Session/Over）、配置选项 |
| `api/internal/` | 内部实现：TableEnvironmentImpl、TableImpl、StatementSetImpl |
| `operations/` | **Operation 树**：57 个操作节点，涵盖 DDL（CreateTable、DropTable...）、DQL（Filter、Join、Aggregate...）、DML（SinkModify...）、命令（Set、Reset...） |
| `delegation/` | 3 个核心委托接口：Parser、Planner、Executor |
| `catalog/` | CatalogManager、FunctionCatalog、SchemaResolver 等元数据管理 |
| `expressions/` | 表达式 API：Expressions 静态工厂、ApiExpression |

**Operation 类层次（部分）：**
```
Operation
├── QueryOperation（DQL）
│   ├── FilterQueryOperation
│   ├── JoinQueryOperation
│   ├── AggregateQueryOperation
│   ├── SortQueryOperation
│   ├── SetQueryOperation（UNION/INTERSECT/EXCEPT）
│   ├── ValuesQueryOperation
│   └── CorrelatedFunctionQueryOperation（LATERAL）
├── ModifyOperation（DML）
│   ├── SinkModifyOperation
│   └── CollectModifyOperation
├── DDL Operations
│   ├── CreateTableOperation
│   ├── AlterTableChangeOperation
│   └── DropTableOperation ...
└── ExecutableOperation（可直接执行的操作）
```

### 4.3 flink-table-planner（查询优化核心）

**最大的子模块**，约 3,149 个文件（Java + Scala 混合），基于 Apache Calcite 框架。

#### 4.3.1 计划节点层次（4 层转换）

```
RelNode（Calcite 标准关系代数）
    ↓ 逻辑优化（FlinkLogicalRel）
FlinkLogicalRel（Flink 逻辑计划）
    ↓ 物理优化（物理规则转换）
FlinkPhysicalRel（Flink 物理计划，分 Batch/Stream）
    ↓ ExecNodeGraphGenerator
ExecNode（执行节点，与 Calcite 解耦）
    ↓ translateToPlan
Transformation（Flink 运行时 DAG）
```

#### 4.3.2 优化器设计

基于 Calcite 的 **Program 链式优化**：

**批处理优化链（FlinkBatchProgram）：**
```
1. FlinkDecorrelateProgram     — 子查询解关联
2. FlinkHepProgram             — 启发式规则优化
3. FlinkVolcanoProgram         — 基于代价的优化（CBO）
4. FlinkDynamicPartitionPruning — 动态分区裁剪
5. FlinkRuntimeFilterProgram   — 运行时过滤器
6. FlinkRecomputeStatistics    — 重算统计信息
```

**流处理优化链（FlinkStreamProgram）：**
```
1. FlinkDecorrelateProgram     — 子查询解关联
2. FlinkHepProgram             — 启发式优化
3. FlinkVolcanoProgram         — CBO 优化
4. FlinkChangelogModeInference — Changelog 模式推导
5. FlinkMiniBatchIntervalTrait — Mini-Batch 间隔推导
6. FlinkRelTimeIndicatorProgram— 时间属性处理
```

**核心优化规则分类：**
- `logical/` — 12 个逻辑优化规则（子查询消除、窗口聚合识别、谓词传递等）
- `physical/batch/` — 36 个批处理物理规则（Hash/Sort Join 选择、Sort Agg、Hash Agg 等）
- `physical/stream/` — 30 个流处理物理规则（增量聚合、Changelog 规范化等）

#### 4.3.3 代码生成（codegen/）

Scala 实现，约 80+ 个文件，核心：
- `ExprCodeGenerator` — 表达式代码生成
- `CalcCodeGenerator` — Calc（投影+过滤）代码生成
- `agg/` — 聚合函数代码生成（Hash Agg、Sort Agg、Window Agg）
- `LongHashJoinGenerator` — Hash Join 代码生成
- `NestedLoopJoinCodeGenerator` — Nested Loop Join 代码生成
- `LookupJoinCodeGenerator` — Lookup Join 代码生成
- `MatchCodeGenerator` — CEP 匹配代码生成
- `SortCodeGenerator` — 排序代码生成

#### 4.3.4 ExecNode 体系

解耦 Calcite 的可序列化执行节点（约 167 + 3 个文件）：

| 目录 | 内容 |
|------|------|
| `exec/common/` | 通用 ExecNode（Calc、Correlate、Exchange、Join、Sink、Source 等 20 个） |
| `exec/batch/` | 批处理专用（HashJoin、SortMergeJoin、SortAggregate、HashAggregate 等 46 个） |
| `exec/stream/` | 流处理专用（GroupAggregate、WindowAggregate、DeltaJoin 等） |
| `exec/processor/` | 图处理器（死锁打破、多输入合并、Forward Exchange、动态过滤等） |
| `exec/serde/` | JSON/Smile 序列化（支持 CompiledPlan） |
| `exec/spec/` | 节点规格定义（JoinSpec、OverSpec、SortSpec、MatchSpec 等） |

### 4.4 flink-table-runtime（运行时层）

**约 938 个文件**，分为：

| 包 | 内容 |
|---|------|
| `operators/` | **369 个算子实现**：join（Hash/Sort/Nested Loop/Temporal/Interval/Lookup/Delta）、aggregate（Hash/Sort/Window/Over）、sort、rank、sink、source、deduplicate、match（CEP） |
| `functions/` | 内建函数实现：标量函数（ArrayXxx、MapXxx、StringXxx 等 40+）、聚合函数（First/Last/Collect/ArrayAgg/Percentile 等 20+）、表函数（Unnest、Lookup Cache） |
| `data/` | RowData 的各种实现（BinaryRowData、JoinedRowData、GenericRowData 等 45 个） |
| `hashtable/` | 哈希表实现（用于 Hash Join 和 Hash Aggregate） |
| `typeutils/` | 序列化器实现（RowData、StringData 等 29 个） |
| `generated/` | 代码生成基类（38 个） |
| `strategy/` | 自适应 Join 优化策略（广播 Join、倾斜 Join） |

### 4.5 flink-sql-parser（SQL 解析器）

基于 Calcite 的 JavaCC 解析器，关键扩展：
- `ddl/` — 91 个 DDL 语法节点（CREATE TABLE、ALTER、Materialized Table 等）
- `dml/` — 10 个 DML 语法节点（INSERT、ON CONFLICT 等）
- `dql/` — 32 个 DQL 语法节点（SHOW、DESCRIBE 等）
- `type/` — 6 个类型扩展（Variant、多集等）

### 4.6 flink-sql-gateway（SQL 网关）

提供远程 SQL 提交服务：
- REST API 端点
- Session 管理
- 多 Statement 执行
- 结果集管理

### 4.7 flink-sql-client（SQL 客户端）

交互式 CLI 工具，支持：
- SQL 语句提交
- 结果展示（tableau 格式）
- 会话管理

---

## 5. Catalog 与元数据管理

```
CatalogStore（持久化存储）
    ↓
CatalogManager（元数据统一管理）
├── Catalog（目录接口）
│   ├── GenericInMemoryCatalog
│   └── 外部实现（Hive、JDBC 等）
├── FunctionCatalog（函数注册与解析）
└── ModuleManager（模块管理，如 CoreModule）

核心元数据对象：
- CatalogTable / ResolvedCatalogTable
- CatalogView / ResolvedCatalogView
- CatalogFunction
- CatalogModel（ML 模型）
- CatalogMaterializedTable（物化表）
- ResolvedSchema / Column / WatermarkSpec / UniqueConstraint
```

---

## 6. 新特性亮点（2.3-SNAPSHOT）

根据最近 commit 历史，该版本包含以下重要新特性：

1. **ProcessTableFunction** — 新一代表处理函数，支持通过 CompiledPlan 注册
2. **ML Model 集成** — `CatalogModel`、`ModelDescriptor`、`MLPredictCodeGenerator` — 原生 ML 预测支持
3. **Materialized Table** — 物化表支持（`CatalogMaterializedTable`、`ContinuousRefreshHandler`）
4. **Variant 类型** — 内建 Variant 数据类型和 `variant_get` 函数
5. **Vector Search** — 向量搜索表源（`VectorSearchTableSource`）
6. **自适应 Join 优化** — 运行时广播 Join 和倾斜 Join 优化策略
7. **ON CONFLICT 支持** — INSERT 语句的冲突处理（Upsert 语义）
8. **Delta Join** — 新的增量 Join 算子
9. **Async Calc** — 异步计算节点
10. **Record 类型原生支持** — DataTypeExtractor 支持 Java Record

---

## 7. 设计模式与架构原则

### 7.1 分层解耦

- **API 层**不依赖 Planner 和 Runtime（通过 `delegation/` 接口解耦）
- **Planner** 不依赖具体 Runtime 实现（通过 ExecNode 抽象）
- **Runtime** 不依赖 Planner（只依赖 Common）
- 使用 `flink-table-planner-loader` 实现 **Scala 版本隔离**

### 7.2 SPI 扩展点

- `DynamicTableSourceFactory` / `DynamicTableSinkFactory` — 连接器扩展
- `ParserFactory` — SQL 方言扩展
- `PlannerFactory` / `ExecutorFactory` — Planner 扩展
- `CatalogFactory` / `CatalogStoreFactory` — Catalog 扩展
- `Module` — 函数模块扩展
- `Format` / `DecodingFormat` / `EncodingFormat` — 序列化格式扩展

### 7.3 代码生成策略

Flink Table 大量使用运行时代码生成（Janino 编译），避免了反射开销：
- 表达式计算直接生成 Java 代码
- Join 条件生成内联代码
- 聚合函数生成状态管理代码
- 使用 `flink-table-code-splitter` 自动分割超过 64KB 的方法

### 7.4 流批统一

- 同一套 API（Table/SQL）处理有界和无界数据
- Planner 分为 `BatchPlanner` 和 `StreamPlanner`，共享 `PlannerBase`
- 优化规则分离（Batch/Stream 各有专用规则集）
- ExecNode 分为 common/batch/stream 三层
- Runtime 算子部分共享、部分专用

---

## 8. 依赖关系图

```
flink-table-common
       ↑
flink-table-api-java ←── flink-table-api-scala
       ↑                        ↑
flink-table-api-java-bridge  flink-table-api-scala-bridge
       ↑
flink-table-planner ←── flink-table-runtime
       ↑                     ↑
flink-table-planner-loader    flink-table-code-splitter
       ↑
flink-table-planner-loader-bundle
       ↑
flink-sql-client / flink-sql-gateway
```

**关键约束：**
- 除 `flink-table-planner` 外，任何模块都不应在生产类路径中依赖 `flink-table-runtime`
- 除 `flink-table-planner-loader` 外，任何模块都不应依赖 `flink-table-planner-loader-bundle`
- 任何模块都不应依赖 `flink-table-planner` 或 `flink-table-planner-loader`

---

## 9. 总结

Flink Table 模块是一个精心设计的、高度模块化的关系型查询引擎，具有以下突出特点：

1. **成熟的分层架构** — Common → API → Planner → Runtime 四层清晰分离
2. **基于 Calcite 的强大优化器** — 支持 RBO + CBO，流批各有专用优化链
3. **高效的代码生成** — 通过 Janino 运行时编译消除反射开销
4. **丰富的扩展机制** — 连接器、格式、Catalog、函数均通过 SPI 可插拔
5. **流批真正统一** — 同一 API、同一类型系统、部分共享的优化和执行
6. **持续演进** — 正在积极发展 ML 集成、物化表、向量搜索等新能力
