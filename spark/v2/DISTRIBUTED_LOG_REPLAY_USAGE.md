# Distributed Log Replay 使用指南

## 快速开始

### 1. 基本使用

```java
import io.delta.spark.internal.v2.read.DistributedLogReplayHelper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// 创建SparkSession
SparkSession spark = SparkSession.builder()
    .appName("Delta V2 Distributed Replay")
    .getOrCreate();

// 获取Delta snapshot
Snapshot snapshot = ...; // 从Table.forPath()或SnapshotBuilder获取

// 执行分布式log replay
Dataset<Row> addFiles = DistributedLogReplayHelper.distributedLogReplay(
    spark,
    snapshot,
    50  // num partitions
);

// 查看结果
addFiles.show();
addFiles.count(); // 文件总数
```

### 2. 集成到SparkScan

在`SparkScan.java`中修改`planScanFiles()`方法：

```java
// 在SparkScan类中添加配置
private static final boolean USE_DISTRIBUTED_REPLAY = 
    Boolean.parseBoolean(System.getProperty(
        "spark.databricks.delta.v2.distributedLogReplay.enabled", 
        "false"));

private static final int DISTRIBUTED_REPLAY_FILE_THRESHOLD = 
    Integer.parseInt(System.getProperty(
        "spark.databricks.delta.v2.distributedLogReplay.fileThreshold",
        "10000"));

private void planScanFiles() {
    // 判断是否使用分布式方案
    boolean useDistributed = USE_DISTRIBUTED_REPLAY && 
        shouldUseDistributedReplay();
    
    if (useDistributed) {
        planScanFilesDistributed();
    } else {
        planScanFilesWithKernel();
    }
}

private boolean shouldUseDistributedReplay() {
    // 简单估算：如果delta文件数量超过阈值，使用分布式
    if (initialSnapshot instanceof SnapshotImpl) {
        SnapshotImpl impl = (SnapshotImpl) initialSnapshot;
        LogSegment logSegment = impl.getLogSegment();
        int estimatedFiles = logSegment.getDeltas().size() * 1000; // 粗略估算
        return estimatedFiles > DISTRIBUTED_REPLAY_FILE_THRESHOLD;
    }
    return false;
}

private void planScanFilesDistributed() {
    SparkSession spark = SparkSession.active();
    
    // Step 1: 分布式log replay
    Dataset<Row> addFiles = DistributedLogReplayHelper.distributedLogReplay(
        spark,
        initialSnapshot,
        50  // numPartitions
    );
    
    // Step 2: 解析stats
    Dataset<Row> withStats = addFiles.withColumn("stats_parsed",
        from_json(col("stats"), DistributedLogReplayHelper.getStatsSchema()));
    
    // Step 3: 应用filters
    Dataset<Row> filtered = applyFiltersOnDataFrame(withStats);
    
    // Step 4: Collect结果
    List<Row> files = filtered.collectAsList();
    
    // Step 5: 转换为PartitionedFile
    for (Row row : files) {
        String path = row.getAs("path");
        long size = row.getAs("size");
        scala.collection.immutable.Map<String, String> partVals = row.getAs("partitionValues");
        
        // 构建PartitionedFile（使用现有的PartitionUtils）
        // ... (与现有逻辑相同)
        
        totalBytes += size;
        partitionedFiles.add(partitionedFile);
    }
    
    planned = true;
}

private void planScanFilesWithKernel() {
    // 保留现有的Kernel实现
    final Engine tableEngine = DefaultEngine.create(hadoopConf);
    final Iterator<FilteredColumnarBatch> scanFileBatches = 
        kernelScan.getScanFiles(tableEngine);
    // ... (现有代码)
}

private Dataset<Row> applyFiltersOnDataFrame(Dataset<Row> df) {
    // 应用partition filters
    for (Filter filter : pushedToKernelFilters) {
        if (isPartitionFilter(filter)) {
            df = df.filter(convertFilterToSparkExpr(filter));
        }
    }
    
    // 应用data skipping filters
    for (Filter filter : dataFilters) {
        df = df.filter(convertToDataSkippingExpr(filter));
    }
    
    return df;
}
```

## 性能优化建议

### 1. 调整分区数量

```properties
# 根据表大小和集群大小调整
# 小表（< 10K files）：10-20 partitions
spark.databricks.delta.v2.distributedLogReplay.numPartitions = 20

# 中表（10K-100K files）：30-50 partitions
spark.databricks.delta.v2.distributedLogReplay.numPartitions = 50

# 大表（> 100K files）：50-100 partitions
spark.databricks.delta.v2.distributedLogReplay.numPartitions = 100
```

### 2. 自适应策略

```java
private int getNumPartitions() {
    if (initialSnapshot instanceof SnapshotImpl) {
        SnapshotImpl impl = (SnapshotImpl) initialSnapshot;
        LogSegment logSegment = impl.getLogSegment();
        int deltaCount = logSegment.getDeltas().size();
        
        // 自适应分区数
        if (deltaCount < 10) return 10;
        if (deltaCount < 50) return 20;
        if (deltaCount < 100) return 50;
        return 100;
    }
    return 50; // default
}
```

### 3. 缓存中间结果

对于频繁查询的表，可以缓存log replay结果：

```java
Dataset<Row> addFiles = DistributedLogReplayHelper.distributedLogReplay(
    spark, snapshot, numPartitions
);

// 缓存结果（如果后续有多个查询）
addFiles.cache();
addFiles.count(); // 触发缓存
```

## 监控和调试

### 1. 性能监控

```java
long startTime = System.nanoTime();

Dataset<Row> addFiles = DistributedLogReplayHelper.distributedLogReplay(
    spark, snapshot, numPartitions
);

long replayTime = System.nanoTime() - startTime;
System.out.println("Log replay time: " + replayTime / 1_000_000 + " ms");

List<Row> files = addFiles.collectAsList();
long totalTime = System.nanoTime() - startTime;
System.out.println("Total time: " + totalTime / 1_000_000 + " ms");
System.out.println("Files found: " + files.size());
```

### 2. Spark UI查看

- 打开Spark UI: http://localhost:4040
- 查看SQL tab，找到distributed log replay的stage
- 检查：
  - Task数量是否等于numPartitions
  - Task分布是否均匀
  - Shuffle read/write量

### 3. 日志调试

```java
// 开启DEBUG日志
spark.sparkContext().setLogLevel("DEBUG");

// 查看DataFrame的执行计划
addFiles.explain(true);

// 查看实际的文件数量
System.out.println("Checkpoint files: " + logSegment.getCheckpoints().size());
System.out.println("Delta files: " + logSegment.getDeltas().size());
```

## 常见问题

### Q1: 为什么小表反而变慢了？

**A**: 分布式处理有task scheduling overhead。对于小表（< 1000 files），串行处理更快。

**解决方案**：使用自适应策略，根据文件数量选择处理方式。

```java
if (estimatedFiles < DISTRIBUTED_REPLAY_FILE_THRESHOLD) {
    planScanFilesWithKernel(); // 串行
} else {
    planScanFilesDistributed(); // 分布式
}
```

### Q2: 如何确保log replay正确性？

**A**: 关键是保证add/remove reconciliation正确。

```java
// 关键代码
Dataset<Row> replayed = allActions
    .groupBy("file_path_canonical")
    .agg(
        last("add", true).as("add"),       // ignoreNulls=true
        last("remove", true).as("remove")
    )
    .filter(col("add").isNotNull().and(col("remove").isNull()));
```

**测试**：
1. 创建测试表，多次add/remove同一个文件
2. 对比Kernel和分布式方案的结果
3. 确保文件列表和stats完全一致

### Q3: Deletion Vector如何处理？

**A**: Deletion Vector信息在AddFile的`deletionVector`字段中。

```java
// 确保保留DV字段
Dataset<Row> addFiles = replayed.select(
    col("add.path"),
    col("add.size"),
    col("add.stats"),
    col("add.deletionVector")  // 重要！
);

// 计算logical records
withStats.withColumn("numLogicalRecords",
    when(col("deletionVector").isNull(),
        col("stats_parsed.numRecords"))
    .otherwise(
        col("stats_parsed.numRecords")
            .minus(col("deletionVector.cardinality"))
    )
);
```

### Q4: V2 Checkpoint如何利用？

**A**: V2 Checkpoint已经有structured stats，可以直接用predicate pushdown。

```java
// 检测V2 checkpoint
boolean isV2Checkpoint = checkpointPath.contains(".checkpoint.parquet");

if (isV2Checkpoint) {
    // V2 checkpoint - 直接filter
    Dataset<Row> checkpointDF = spark.read()
        .format("parquet")
        .load(checkpointPath)
        .filter("add.stats_parsed.maxValues.age > 18");  // Parquet pushdown!
} else {
    // V1 checkpoint - 需要手动解析
    Dataset<Row> checkpointDF = spark.read()
        .format("parquet")
        .load(checkpointPath)
        .withColumn("stats_parsed", from_json(col("add.stats"), statsSchema))
        .filter(col("stats_parsed.maxValues.age").gt(18));
}
```

## 与V1对比

| 特性 | V1 (PrepareDeltaScan) | V2 (Distributed Replay) | 状态 |
|------|----------------------|-------------------------|------|
| 分布式log replay | ✅ | ✅ | ✅ 已实现 |
| 分布式stats解析 | ✅ | ✅ | ✅ 已实现 |
| Partition filtering | ✅ | ⚠️ | 🚧 需完善 |
| Data skipping | ✅ | ⚠️ | 🚧 需完善 |
| IsNull expansion | ✅ | ❌ | 📋 待实现 |
| StartsWith optimization | ✅ | ❌ | 📋 待实现 |
| Generated columns | ✅ | ❌ | 📋 待实现 |
| Limit pushdown | ✅ | ❌ | 📋 待实现 |
| Metadata-only queries | ✅ | ❌ | 📋 待实现 |

## 下一步

1. **完善data skipping filters**
   - 实现完整的filter conversion
   - 支持所有V1的优化

2. **性能测试**
   - 不同表大小的benchmark
   - 与V1性能对比

3. **集成测试**
   - 各种Delta表格式
   - 各种查询模式

4. **生产化**
   - 错误处理
   - 监控指标
   - 配置管理
