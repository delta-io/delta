# V2 Distributed Log Replay 测试指南

## 算法已完成 ✅

V2的distributed log replay已经**完全遵循V1的DataFrame算法**，包括：
- ✅ Snapshot.stateReconstruction 算法（log replay）
- ✅ DataSkippingReader.withStats 算法（stats解析）
- ✅ DataSkippingReader.getDataSkippedFiles 算法（data skipping）

详见：`V1_VS_V2_ALGORITHM_COMPARISON.md`

## 测试计划

### Phase 1: 单元测试（立即可做）

```scala
// Test file: DistributedLogReplayHelperSuite.scala

class DistributedLogReplayHelperSuite extends QueryTest with SharedSparkSession {
  
  test("stateReconstructionV2 matches V1 result") {
    withTempDir { dir =>
      // Create test table
      createTestTable(dir, numFiles = 100)
      
      // V1 approach (via Snapshot.allFiles)
      val v1Snapshot = DeltaLog.forTable(spark, dir).snapshot
      val v1Files = v1Snapshot.allFiles.collect()
      
      // V2 approach (via DistributedLogReplayHelper)
      val kernelSnapshot = Table.forPath(dir.toString).getLatestSnapshot()
      val v2Files = DistributedLogReplayHelper
        .stateReconstructionV2(spark, kernelSnapshot, 50)
        .collect()
      
      // Compare results
      assert(v1Files.length == v2Files.length)
      // Compare each file
    }
  }
  
  test("withStats parses JSON correctly") {
    withTempDir { dir =>
      createTestTable(dir, numFiles = 10)
      val snapshot = DeltaLog.forTable(spark, dir).snapshot
      val allFiles = snapshot.allFiles.toDF()
      
      val withStats = DistributedLogReplayHelper.withStats(
        allFiles, 
        snapshot.statsSchema
      )
      
      // Verify stats_parsed is not null
      val parsed = withStats
        .select("stats.numRecords")
        .collect()
      
      assert(parsed.forall(!_.isNullAt(0)))
    }
  }
}
```

### Phase 2: 性能基准测试

```scala
// benchmark/DistributedLogReplayBenchmark.scala

object DistributedLogReplayBenchmark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    
    val tableSizes = Seq(100, 1000, 10000, 100000, 1000000)
    
    for (size <- tableSizes) {
      println(s"\n=== Testing table with $size files ===")
      
      val table = createLargeTable(size)
      
      // V1 approach
      val v1Start = System.nanoTime()
      val v1Snapshot = DeltaLog.forTable(spark, table).snapshot
      val v1Files = v1Snapshot.allFiles.collect()
      val v1Time = (System.nanoTime() - v1Start) / 1e6
      println(s"V1 time: $v1Time ms")
      
      // V2 approach
      val v2Start = System.nanoTime()
      val kernelSnapshot = Table.forPath(table).getLatestSnapshot()
      val v2Files = DistributedLogReplayHelper
        .stateReconstructionV2(spark, kernelSnapshot, 50)
        .collect()
      val v2Time = (System.nanoTime() - v2Start) / 1e6
      println(s"V2 time: $v2Time ms")
      
      // Compare
      val speedup = v1Time / v2Time
      println(s"Speedup: ${speedup}x")
      assert(v1Files.length == v2Files.length)
    }
  }
  
  def createLargeTable(numFiles: Int): String = {
    // Create a Delta table with specified number of files
    // ...
  }
}
```

### Phase 3: 集成测试

```scala
// Test full query flow with V2 connector

test("end-to-end query with distributed replay") {
  withTempDir { dir =>
    // Create large partitioned table
    createPartitionedTable(dir, numFiles = 10000)
    
    // Run query via V2 connector
    val df = spark.read
      .format("delta")
      .option("spark.databricks.delta.v2.enabled", "true")
      .option("spark.databricks.delta.v2.distributedLogReplay.enabled", "true")
      .load(dir.toString)
      .filter($"age" > 18)
      .filter($"department" === "Engineering")
    
    val result = df.collect()
    
    // Verify correctness
    val expected = spark.read
      .format("delta")
      .load(dir.toString) // V1
      .filter($"age" > 18)
      .filter($"department" === "Engineering")
      .collect()
    
    assert(result.sameElements(expected))
  }
}
```

### Phase 4: 压力测试

```scala
// Test with very large tables

test("1M files table") {
  withTempDir { dir =>
    // This will take a while to create...
    createLargeTable(dir, numFiles = 1000000)
    
    val v2Start = System.nanoTime()
    val v2Files = DistributedLogReplayHelper
      .stateReconstructionV2(spark, snapshot, 100) // More partitions
      .collect()
    val v2Time = (System.nanoTime() - v2Start) / 1e6
    
    println(s"1M files processed in $v2Time ms")
    assert(v2Time < 15000) // Should be < 15s
  }
}
```

## 快速手动测试

如果你有现成的大表：

```scala
import io.delta.spark.internal.v2.read.DistributedLogReplayHelper

val spark = SparkSession.builder().master("local[*]").getOrCreate()

// Point to your existing Delta table
val tablePath = "/path/to/your/large/delta/table"

// Get Kernel snapshot
val snapshot = io.delta.kernel.Table.forPath(tablePath).getLatestSnapshot()

// Test distributed log replay
val start = System.nanoTime()
val files = DistributedLogReplayHelper
  .stateReconstructionV2(spark, snapshot, 50)
  .collect()
val time = (System.nanoTime() - start) / 1e6

println(s"Files: ${files.length}")
println(s"Time: $time ms")

// Compare with V1
val v1Start = System.nanoTime()
val v1Snapshot = org.apache.spark.sql.delta.DeltaLog.forTable(spark, tablePath).snapshot
val v1Files = v1Snapshot.allFiles.collect()
val v1Time = (System.nanoTime() - v1Start) / 1e6

println(s"V1 Files: ${v1Files.length}")
println(s"V1 Time: $v1Time ms")
println(s"Match: ${files.length == v1Files.length}")
```

## 预期结果

### 正确性
- V1和V2返回的文件列表应该**完全相同**
- 文件path, size, stats等字段应该一致

### 性能
因为算法完全相同，性能应该接近：

| 文件数 | V1预期 | V2预期 | 差异 |
|--------|--------|--------|------|
| 100 | 100ms | 120ms | +20% (Java overhead) |
| 1K | 200ms | 220ms | +10% |
| 10K | 800ms | 850ms | +6% |
| 100K | 2s | 2.1s | +5% |
| 1M | 8s | 8.5s | +6% |

如果差异超过10%，需要调查原因。

## 下一步

1. **先写单元测试** - 验证基本正确性
2. **再做基准测试** - 验证性能
3. **最后集成测试** - 端到端验证

测试通过后，即可合并到主分支！
