# Flink HashMap State 的 Copy-on-Write（COW）机制深度分析

## 1. 问题背景：为什么需要 COW？

Flink 的 Checkpoint 机制需要对状态做**异步快照**：在不停止数据处理的前提下，将某一时刻的状态一致性地写入持久存储。

对于 HashMap State Backend（堆内存状态），面临一个核心矛盾：

```
主线程 (处理数据，持续修改状态)
    │
    │  同时进行
    ▼
异步 IO 线程 (遍历状态，序列化写入存储)
```

如果不做任何处理，异步线程读到的状态可能已经被主线程修改了——破坏快照一致性。

**两种解决方案：**
1. **全量深拷贝**：在触发快照时，把整个状态深拷贝一份给异步线程。问题：内存翻倍，拷贝耗时。
2. **Copy-on-Write**：快照时不拷贝，只有当主线程真正要修改某条状态时，才拷贝那一条。Flink 选择了这种方案。

---

## 2. 核心数据结构

### 2.1 CopyOnWriteStateMap

`CopyOnWriteStateMap<K, N, S>` 是 COW 机制的核心实现，是一个定制的 HashMap，存储结构为：

```
(Key, Namespace) → State

其中 Key 是用户的 key，Namespace 通常是 Window
```

**核心字段：**

```java
public class CopyOnWriteStateMap<K, N, S> {
    // 主哈希表
    StateMapEntry<K, N, S>[] primaryTable;
    // 增量 rehash 时的第二张表
    StateMapEntry<K, N, S>[] incrementalRehashTable;

    // ★ COW 的版本号：每次快照时递增
    int stateMapVersion;
    // ★ 当前仍被未完成的快照所需要的最高版本号
    int highestRequiredSnapshotVersion;
    // ★ 所有活跃快照的版本号集合
    TreeSet<Integer> snapshotVersions;
}
```

### 2.2 StateMapEntry（带版本号的哈希条目）

每个 Entry 携带两个版本号，这是 COW 的精髓：

```java
static class StateMapEntry<K, N, S> {
    final K key;
    final N namespace;
    S state;
    StateMapEntry<K, N, S> next;  // 哈希链
    final int hash;

    // ★ 条目结构版本：记录这个 Entry 链节点本身是在哪个版本创建/复制的
    int entryVersion;
    // ★ 状态值版本：记录 state 对象是在哪个版本写入的
    int stateVersion;
}
```

**两个版本号的分工：**
- `entryVersion`：保护 Entry 的**链表结构**（next 指针）。修改链表结构时，如果 Entry 的版本早于快照版本，需要复制这个 Entry 节点。
- `stateVersion`：保护 Entry 中的**状态值对象**。读取状态时，如果状态版本早于快照版本，需要深拷贝状态对象。

---

## 3. COW 工作原理（逐操作分析）

### 3.1 触发快照：`snapshotMapArrays()`

当 Checkpoint 触发时，主线程**同步**执行：

```java
StateMapEntry<K, N, S>[] snapshotMapArrays() {
    synchronized (snapshotVersions) {
        // 1. 递增版本号
        ++stateMapVersion;  // 例如从 v3 变为 v4

        // 2. 记录快照版本
        highestRequiredSnapshotVersion = stateMapVersion;  // = v4
        snapshotVersions.add(stateMapVersion);
    }

    // 3. 浅拷贝哈希桶数组（只拷贝指针数组，不拷贝 Entry 和 State）
    StateMapEntry<K, N, S>[] copy = new StateMapEntry[...];
    System.arraycopy(primaryTable, 0, copy, 0, primaryTable.length);

    return copy;  // 交给异步线程序列化
}
```

**关键点：** 只拷贝了桶数组的引用，没有拷贝任何 Entry 或 State 对象。此时快照数据和活跃数据**共享**所有 Entry 和 State 对象。

### 3.2 读取状态：`get(key, namespace)` — 惰性拷贝

```java
S get(K key, N namespace) {
    // ... 找到 Entry e ...
    
    // ★ COW 检查：状态值的版本 < 快照需要的版本？
    if (e.stateVersion < highestRequiredSnapshotVersion) {
        
        // ★ COW 检查：Entry 结构的版本也需要保护？
        if (e.entryVersion < highestRequiredSnapshotVersion) {
            // 复制 Entry 节点本身（保护链表结构）
            e = handleChainedEntryCopyOnWrite(tab, index, e);
        }
        
        // ★ 深拷贝状态值对象
        e.state = stateSerializer.copy(e.state);
        // 更新状态版本为当前版本
        e.stateVersion = stateMapVersion;
    }
    
    return e.state;  // 返回给用户的是拷贝后的新对象
}
```

**为什么 get 也要拷贝？** 因为用户拿到 state 引用后可能会修改它（例如 `state.add(element)`）。如果不拷贝，用户修改会污染快照线程正在读取的数据。

### 3.3 写入状态：`put(key, namespace, value)`

```java
void put(K key, N namespace, S value) {
    StateMapEntry<K, N, S> e = putEntry(key, namespace);
    
    // 直接替换状态值（新对象，不需要拷贝旧的）
    e.state = value;
    e.stateVersion = stateMapVersion;
}

private StateMapEntry<K, N, S> putEntry(K key, N namespace) {
    // ... 查找 Entry ...
    for (StateMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
        if (匹配) {
            // ★ COW 检查：修改前确保 Entry 链节点是当前版本的
            if (e.entryVersion < highestRequiredSnapshotVersion) {
                e = handleChainedEntryCopyOnWrite(tab, index, e);
            }
            return e;
        }
    }
    // 不存在则创建新 Entry，版本号 = stateMapVersion
    return addNewStateMapEntry(tab, key, namespace, hash);
}
```

### 3.4 删除状态：`removeEntry(key, namespace)`

```java
private StateMapEntry<K, N, S> removeEntry(K key, N namespace) {
    for (StateMapEntry<K, N, S> e = tab[index], prev = null; e != null; prev = e, e = e.next) {
        if (匹配) {
            if (prev == null) {
                tab[index] = e.next;  // 头节点直接修改桶指针
            } else {
                // ★ 修改 prev.next 前，先 COW 保护 prev 节点
                if (prev.entryVersion < highestRequiredSnapshotVersion) {
                    prev = handleChainedEntryCopyOnWrite(tab, index, prev);
                }
                prev.next = e.next;
            }
            return e;
        }
    }
}
```

### 3.5 链式 Entry 的 COW：`handleChainedEntryCopyOnWrite`

这是最精妙的部分——当需要修改链表中的某个节点时，必须复制从链表头到该节点的所有旧版本节点：

```java
private StateMapEntry<K, N, S> handleChainedEntryCopyOnWrite(
        StateMapEntry<K, N, S>[] tab, int mapIdx, StateMapEntry<K, N, S> untilEntry) {

    StateMapEntry<K, N, S> current = tab[mapIdx];  // 链表头
    StateMapEntry<K, N, S> copy;

    // 从链表头开始，逐个检查和复制
    if (current.entryVersion < required) {
        copy = new StateMapEntry<>(current, stateMapVersion);  // 复制节点
        tab[mapIdx] = copy;  // 更新桶指针
    } else {
        copy = current;
    }

    // 沿链表复制到目标节点
    while (current != untilEntry) {
        current = current.next;
        if (current.entryVersion < required) {
            copy.next = new StateMapEntry<>(current, stateMapVersion);
            copy = copy.next;
        } else {
            copy = current;
        }
    }

    return copy;  // 返回目标节点的新副本
}
```

**图示：**

```
快照时（v3）的链表状态：
桶[i] → A(v2) → B(v2) → C(v2) → null
                          ↑ 快照线程仍在读取这些节点

主线程要修改 C：
桶[i] → A'(v4) → B'(v4) → C'(v4) → null    ← 主线程的新视图
         ↓          ↓
         A(v2) →   B(v2)  → C(v2) → null    ← 快照线程的旧视图（不受影响）
```

### 3.6 释放快照：`releaseSnapshot`

当快照写入完成后，通知 StateMap 释放该版本：

```java
void releaseSnapshot(int snapshotVersion) {
    synchronized (snapshotVersions) {
        snapshotVersions.remove(snapshotVersion);
        // 更新最高需要保护的版本
        highestRequiredSnapshotVersion = snapshotVersions.isEmpty() 
            ? 0              // 没有活跃快照，不需要 COW
            : snapshotVersions.last();
    }
}
```

一旦 `highestRequiredSnapshotVersion` 变为 0，所有后续操作都不会触发任何拷贝——零开销。

---

## 4. 整体流程图

```
═══════════════════════════════════════════════════════════════
时间线
═══════════════════════════════════════════════════════════════

主线程:    ─── put/get ───┬── stateMapVersion++ ──┬── put/get (触发COW) ──┬── put/get (无COW) ──
                          │                       │                      │
                   snapshotMapArrays()     releaseSnapshot()
                   (浅拷贝桶数组)          (版本号清除)
                          │                       │
异步IO线程:               └── 遍历快照数据 ────────┘
                              (序列化写入)
                              读到的是快照时刻的一致视图

═══════════════════════════════════════════════════════════════
```

## 5. 层次结构

```
HashMapStateBackend
    │
    ▼
HeapKeyedStateBackend
    │
    ├── HeapValueState / HeapMapState / ...  (用户 API 层)
    │
    ├── CopyOnWriteStateTable<K, N, S>       (per state，按 KeyGroup 分片)
    │       │
    │       ├── CopyOnWriteStateMap[0]       (KeyGroup 0 的所有 entries)
    │       ├── CopyOnWriteStateMap[1]       (KeyGroup 1)
    │       └── ...
    │
    ├── HeapSnapshotStrategy                 (快照策略)
    │       │
    │       ├── syncPrepareResources()       (同步：调用各 StateTable.stateSnapshot())
    │       │                                 → CopyOnWriteStateTable.stateSnapshot()
    │       │                                   → CopyOnWriteStateMap.snapshotMapArrays()
    │       │                                     (浅拷贝桶数组 + 递增版本号)
    │       │
    │       └── asyncSnapshot()              (异步：遍历快照数据，序列化写出)
    │                                         → CopyOnWriteStateMapSnapshot.writeState()
    │                                           (遍历 snapshotData 数组，序列化每个 Entry)
    │
    └── releaseSnapshot()                    (快照完成，释放版本保护)
```

## 6. 设计精妙之处

### 6.1 双版本号设计

将 Entry 结构（链表指针）和 State 值分开用两个版本号跟踪：

- 如果只修改 state 值（`put` 一个全新对象），只需要保护 state → 只拷贝 state
- 如果修改链表结构（`remove` 中间节点），需要保护链表 → 拷贝 Entry 节点
- 如果 `get` 后用户可能原地修改 state，需要同时保护 → 拷贝两者

这种细粒度跟踪**最小化了拷贝量**。

### 6.2 增量 Rehash

`CopyOnWriteStateMap` 不像 `java.util.HashMap` 那样一次性 rehash，而是使用**增量 rehash**：

```java
// 每次操作时迁移少量条目
while (transferred < MIN_TRANSFERRED_PER_INCREMENTAL_REHASH) {
    // 从 primaryTable[rehashIndex] 迁移到 incrementalRehashTable
}
```

这避免了 rehash 时的大量复制与快照的冲突，保证了 COW 语义在 rehash 期间也能正确工作。

### 6.3 支持多个并发快照

通过 `TreeSet<Integer> snapshotVersions`，支持同时有多个活跃快照：

```java
// 版本号递增：v1 → v2 → v3
// 可能同时有 v2 和 v3 的快照未完成
// highestRequiredSnapshotVersion = max(活跃版本) = v3
// 任何 version < v3 的 Entry/State 都需要 COW 保护
```

### 6.4 零开销常态

没有快照进行时：`highestRequiredSnapshotVersion = 0`，所有 Entry 的版本号 >= 0，因此所有 COW 检查条件 `e.stateVersion < 0` 永远为 false。常规操作**零额外开销**。

---

## 7. 与其他方案的对比

| 方案 | 内存开销 | 同步阻塞 | 实现复杂度 |
|------|---------|---------|-----------|
| **全量深拷贝** | O(N) 额外内存 | 长（拷贝全部状态）| 低 |
| **COW（Flink 方案）** | 仅修改的部分 | 极短（只拷贝桶数组）| 高 |
| **RocksDB 快照** | 几乎为零（LSM-Tree 天然不可变）| 极短 | 中（依赖 RocksDB 实现）|

Flink 的 COW 方案在**纯 Java 堆内存**的约束下，实现了近似于 RocksDB 快照的低开销异步快照能力，代价是实现复杂度较高（版本跟踪 + 链表复制 + 增量 rehash）。

---

## 8. 注意事项

1. **用户不应缓存状态引用**：COW 只保证在 `processElement` 调用周期内的引用有效，跨调用持有引用可能导致不一致。
2. **状态对象需要可序列化拷贝**：COW 依赖 `TypeSerializer.copy()` 来深拷贝状态值。
3. **快照期间写密集场景**：如果快照期间大量 Entry 被修改，会产生较多拷贝。但这仍远好于全量深拷贝。
4. **只增不缩**：当前实现中，CopyOnWriteStateMap 只会增长，不会在负载降低时缩小哈希表。
