# Kernel API 提案：允许使用更新版本的 Snapshot 读取历史数据

## 问题背景

当前 Kernel API 强制要求 `startSnapshot.version == startVersion`：

```java
// CommitRangeImpl.java line 125-127
checkArgument(
    startSnapshot.getVersion() == startVersion,
    "startSnapshot must have version = startVersion");
```

这导致：
1. 必须能够重建 startVersion 的 snapshot
2. 如果 startVersion 无法重建（missing checkpoint, unsupported protocol），则无法读取
3. 无法实现 V1 的"用最新 P&M 读历史数据"策略

## 提案：允许 `snapshot.version >= startVersion`

### 核心修改

```java
// 修改前 (CommitRangeImpl.java line 125-127)
checkArgument(
    startSnapshot.getVersion() == startVersion,
    "startSnapshot must have version = startVersion");

// 修改后
checkArgument(
    startSnapshot.getVersion() >= startVersion,
    "startSnapshot version must be >= startVersion");
```

### 语义变化

**修改前**：
- 必须用 startVersion 的 P&M 来读取 startVersion 的数据
- 保证每个版本用其自己的 P&M 解析

**修改后**：
- 可以用 **任何 >= startVersion** 的 P&M 来读取 startVersion 及之后的数据
- 允许用最新版本的 P&M 读取历史数据（类似 V1）
- 仍然保留用历史版本 P&M 的选项（如果可用）

## 使用场景

### 场景 1: startVersion 无法重建（当前问题）

```java
// 当前行为：失败
try {
  Snapshot snapshot1 = loadSnapshotAt(1);  // 失败！protocol unsupported
  commitRange.getActions(engine, snapshot1, actionSet);
} catch (Exception e) {
  // 无法读取任何数据
}

// 提案后：使用最新 snapshot
Snapshot latestSnapshot = loadLatestSnapshot();  // 成功
commitRange.getActions(engine, latestSnapshot, actionSet);
// 用最新 P&M 读取 version 1 的数据
```

### 场景 2: 性能优化

不需要为每个 startVersion 重建 snapshot，可以复用缓存的 latest snapshot。

```java
// 当前：每次都要重建 snapshot
Snapshot snap1 = loadSnapshotAt(1);
Snapshot snap2 = loadSnapshotAt(2);
Snapshot snap3 = loadSnapshotAt(3);

// 提案后：可以复用
Snapshot latest = loadLatestSnapshot();  // 只加载一次
commitRange1.getActions(engine, latest, actionSet);
commitRange2.getActions(engine, latest, actionSet);
commitRange3.getActions(engine, latest, actionSet);
```

### 场景 3: 灵活选择策略

```java
// 策略 1: 精确模式（更安全）
if (canReconstructSnapshot(startVersion)) {
  Snapshot exactSnapshot = loadSnapshotAt(startVersion);
  commitRange.getActions(engine, exactSnapshot, actionSet);
}

// 策略 2: 最新模式（更实用）
else {
  Snapshot latestSnapshot = loadLatestSnapshot();
  commitRange.getActions(engine, latestSnapshot, actionSet);
  // 假设 P&M 向前兼容
}
```

## 优势

### 1. 解决 startingVersion 测试失败

DSv2 可以实现类似 V1 的行为：

```java
// SparkMicroBatchStream.java
private CloseableIterator<IndexedFile> filterDeltaLogs(
    long startVersion, Optional<DeltaSourceOffset> endOffset) {
  
  CommitRange commitRange = snapshotManager.getTableChanges(...);
  
  // 尝试加载 startVersion 的 snapshot
  Snapshot startSnapshot;
  try {
    startSnapshot = snapshotManager.loadSnapshotAt(startVersion);
  } catch (KernelException e) {
    // 回退到最新 snapshot（类似 V1）
    startSnapshot = snapshotManager.loadLatestSnapshot();
    logWarning("Using latest snapshot P&M to read from version " + startVersion);
  }
  
  // 现在可以工作了！
  commitRange.getActions(engine, startSnapshot, ACTION_SET);
}
```

### 2. 保持 API 结构

不需要：
- 改变 API 签名（仍然需要 snapshot 参数）
- 添加复杂的 Optional 逻辑
- 破坏现有代码

只需要：
- 放松一个 validation 检查
- 允许更灵活的使用方式

### 3. 向后兼容

所有现有代码仍然工作：
```java
// 现有代码（exact version）仍然有效
Snapshot snapshot = loadSnapshotAt(startVersion);
commitRange.getActions(engine, snapshot, actionSet);
// ✅ snapshot.version == startVersion，检查通过
```

新代码可以使用更灵活的方式：
```java
// 新代码（latest version）
Snapshot latest = loadLatestSnapshot();
commitRange.getActions(engine, latest, actionSet);
// ✅ latest.version >= startVersion，检查也通过
```

### 4. 用户可选择安全级别

```java
// 配置选项
if (spark.conf.get("spark.delta.streaming.useExactVersionSnapshot")) {
  // 严格模式：必须用精确版本
  snapshot = loadSnapshotAt(startVersion);
} else {
  // 宽松模式：允许用最新版本
  snapshot = loadLatestSnapshot();
}
```

## 风险和限制

### 风险 1: P&M 不向前兼容

**问题**：新版本的 P&M 可能无法正确读取老版本的 AddFile

**例子**：
```
Version 1: No column mapping
  AddFile.stats = {"id": {min: 0, max: 99}}

Version 10: Column mapping enabled
  Metadata expects physical names "col-abc123"
  
// 用 version 10 的 P&M 读取 version 1 的 AddFile
// 可能误解 "id" 作为物理名而不是逻辑名
```

**缓解**：
1. 文档明确说明风险
2. 添加警告日志
3. 提供配置选项让用户选择
4. 在已知不兼容的情况下（如 column mapping 启用前后）抛出错误

### 风险 2: 协议版本不匹配

**问题**：更新的 snapshot 可能有更新的 protocol 要求

**例子**：
```
Version 1: Reader=1, Writer=2
Version 10: Reader=3, Writer=7, features=[deletionVectors]

// 用 version 10 的 protocol 读取 version 1 的数据
// Version 1 的 AddFile 没有 deletionVector 字段
// 应该没问题，新 protocol 应该能读旧格式
```

**缓解**：
- 协议设计上应该保证向前兼容
- 如果不兼容，在 AddFile 解析时会失败（fail fast）

### 风险 3: 语义混淆

**问题**：用户可能不理解为什么同一个 AddFile 用不同的 snapshot 读取结果不同

**缓解**：
- 详细文档说明行为
- 明确的日志和警告
- 提供配置选项控制行为

## 实现建议

### Phase 1: 最小修改（只放松检查）

```java
// CommitRangeImpl.java
private void validateParameters(
    Engine engine, Snapshot startSnapshot, Set<DeltaLogActionUtils.DeltaAction> actionSet) {
  requireNonNull(engine, "engine cannot be null");
  requireNonNull(startSnapshot, "startSnapshot cannot be null");
  requireNonNull(actionSet, "actionSet cannot be null");
  
  // 修改这里
  checkArgument(
      startSnapshot.getVersion() >= startVersion,
      "startSnapshot version must be >= startVersion (got %s, expected >= %s)",
      startSnapshot.getVersion(), startVersion);
}
```

### Phase 2: 添加警告

```java
private void validateParameters(...) {
  // ... 基本检查 ...
  
  if (startSnapshot.getVersion() > startVersion) {
    logger.warn(
      "Using snapshot at version {} to read commits starting from version {}. " +
      "This assumes Protocol/Metadata are forward compatible. " +
      "If you encounter issues, try using exactVersionSnapshot=true",
      startSnapshot.getVersion(), startVersion);
  }
}
```

### Phase 3: 添加配置选项（DSv2 层）

```java
// SparkMicroBatchStream.java
private CloseableIterator<IndexedFile> filterDeltaLogs(...) {
  boolean useExactVersion = sqlConf.getConf(
    DeltaSQLConf.DELTA_STREAMING_DSV2_USE_EXACT_VERSION_SNAPSHOT);
  
  Snapshot startSnapshot;
  if (useExactVersion) {
    // 严格模式：必须精确版本
    startSnapshot = snapshotManager.loadSnapshotAt(startVersion);
  } else {
    // 宽松模式：尝试精确版本，失败则用最新
    try {
      startSnapshot = snapshotManager.loadSnapshotAt(startVersion);
    } catch (KernelException e) {
      logger.warn("Cannot load snapshot at version {}, using latest", startVersion);
      startSnapshot = snapshotManager.loadLatestSnapshot();
    }
  }
  
  commitRange.getActions(engine, startSnapshot, ACTION_SET);
}
```

### Phase 4: 智能检测不兼容情况

```java
private void validateProtocolCompatibility(
    Snapshot snapshot, long startVersion, long endVersion) {
  
  // 检查是否有重大 P&M 变化
  if (hasColumnMappingChanged(snapshot, startVersion)) {
    throw new DeltaException(
      "Column mapping mode changed between version " + startVersion + 
      " and " + snapshot.getVersion() + ". " +
      "Cannot safely use newer snapshot to read older data. " +
      "Please use exactVersionSnapshot=true");
  }
  
  // 其他不兼容检查...
}
```

## 完整的解决方案对比

### 方案 A: 当前状态（严格要求 exact version）
- ✅ 保证正确性
- ❌ 无法处理无法重建的 snapshot
- ❌ startingVersion 测试失败

### 方案 B: 完全移除 startSnapshot 参数
- ✅ 最灵活
- ❌ 破坏 API
- ❌ 失去协议验证能力

### 方案 C: 本提案（允许 >= startVersion）
- ✅ 保持 API 结构
- ✅ 解决无法重建的问题
- ✅ 向后兼容
- ✅ 用户可选择策略
- ⚠️ 需要文档说明风险
- ⚠️ 假设 P&M 向前兼容

### 方案 D: 方案 C + 智能检测
- ✅ 方案 C 的所有优点
- ✅ 自动检测不兼容情况
- ✅ 在安全时允许，不安全时拒绝
- ⚠️ 实现更复杂

## 推荐

**推荐采用方案 D（分阶段实施）：**

1. **Phase 1（立即）**: 修改 validation 检查，允许 `>= startVersion`
2. **Phase 2（短期）**: 添加警告日志，提醒用户风险
3. **Phase 3（中期）**: 在 DSv2 添加配置选项，默认使用宽松模式
4. **Phase 4（长期）**: 添加智能检测，自动识别不兼容情况

这样可以：
- 立即解决 startingVersion 测试失败问题
- 保持足够的灵活性
- 随时间推移增加安全检查
- 让用户选择最适合的策略

## 文档要求

必须明确文档说明：

1. **默认行为**：Kernel 允许使用更新版本的 snapshot
2. **假设**：假设 Protocol/Metadata 是向前兼容的
3. **风险**：如果 P&M 发生不兼容变化（如启用 column mapping），可能产生错误结果
4. **配置**：提供 `exactVersionSnapshot` 选项强制使用精确版本
5. **最佳实践**：建议在生产环境使用精确版本模式

## 总结

**你的建议非常有价值！**

通过允许 `snapshot.version >= startVersion`，我们可以：
- ✅ 解决 DSv2 的 startingVersion 测试失败
- ✅ 实现类似 V1 的灵活性
- ✅ 保持 Kernel API 的结构
- ✅ 保留精确版本的选项（向后兼容）
- ✅ 让用户根据场景选择策略

这是一个**简单但强大**的修改，只需要改一行代码，但能解决根本性的架构限制。

