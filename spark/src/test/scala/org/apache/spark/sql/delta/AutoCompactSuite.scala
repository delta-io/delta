/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import java.io.File


// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.optimize._
import org.apache.spark.sql.delta.hooks.{AutoCompact, AutoCompactType}
import org.apache.spark.sql.delta.optimize.CompactionTestHelperForAutoCompaction
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.AutoCompactPartitionStats
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

trait AutoCompactTestUtils {
  def captureOptimizeLogs(metrics: String)(f: => Unit): Seq[UsageRecord] = {
    val usageLogs = Log4jUsageLogger.track(f)
    usageLogs.filter { usageLog =>
      usageLog.tags.get("opType") == Some(metrics)
    }
  }

}


/**
 * This class extends the [[CompactionSuiteBase]] and runs all the [[CompactionSuiteBase]] tests
 * with AutoCompaction.
 *
 * It also tests AutoCompaction specific behavior around configuration settings.
 */
class AutoCompactConfigurationSuite extends
    CompactionTestHelperForAutoCompaction
  with DeltaSQLCommandTest
  with SharedSparkSession
  with AutoCompactTestUtils {

  private def setTableProperty(log: DeltaLog, key: String, value: String): Unit = {
    spark.sql(s"ALTER TABLE delta.`${log.dataPath}` SET TBLPROPERTIES " +
      s"($key = $value)")
  }

  test("auto-compact-type: test table properties") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      spark.range(0, 1).write.format("delta").mode("append").save(dir)
      val deltaLog = DeltaLog.forTable(spark, dir)
      val defaultAutoCompactType = AutoCompact.getAutoCompactType(conf, deltaLog.snapshot.metadata)
      Map(
        "true" -> Some(AutoCompactType.Enabled),
        "tRue" -> Some(AutoCompactType.Enabled),
        "'true'" -> Some(AutoCompactType.Enabled),
        "false" -> None,
        "fALse" -> None,
        "'false'" -> None
      ).foreach { case (propertyValue, expectedAutoCompactType) =>
        setTableProperty(deltaLog, "delta.autoOptimize.autoCompact", propertyValue)
        assert(AutoCompact.getAutoCompactType(conf, deltaLog.snapshot.metadata) ==
          expectedAutoCompactType)
      }
    }
  }

  test("auto-compact-type: test confs") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      spark.range(0, 1).write.format("delta").mode("append").save(dir)
      val deltaLog = DeltaLog.forTable(spark, dir)
      val defaultAutoCompactType = AutoCompact.getAutoCompactType(conf, deltaLog.snapshot.metadata)

      Map(
        "true" -> Some(AutoCompactType.Enabled),
        "TrUE" -> Some(AutoCompactType.Enabled),
        "false" -> None,
        "FalsE" -> None
      ).foreach { case (confValue, expectedAutoCompactType) =>
        withSQLConf(DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> confValue) {
          assert(AutoCompact.getAutoCompactType(conf, deltaLog.snapshot.metadata) ==
            expectedAutoCompactType)
        }
      }
    }
  }

}

/**
 * This class extends the [[CompactionSuiteBase]] and runs all the [[CompactionSuiteBase]] tests
 * with AutoCompaction.
 *
 * It also tests AutoCompaction specific behavior around compaction execution.
 */
class AutoCompactExecutionSuite extends
    CompactionTestHelperForAutoCompaction
  with DeltaSQLCommandTest
  with SharedSparkSession
  with AutoCompactTestUtils {
  private def testBothModesViaProperty(testName: String)(f: String => Unit): Unit = {
    def runTest(autoCompactConfValue: String): Unit = {
      withTempDir { dir =>
        withSQLConf(
            "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact" ->
              s"$autoCompactConfValue",
            DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0",
            DeltaSQLConf.DELTA_AUTO_COMPACT_MODIFIED_PARTITIONS_ONLY_ENABLED.key -> "false") {
          f(dir.getCanonicalPath)
        }
      }
    }

    test(s"auto-compact-enabled-property: $testName") { runTest(autoCompactConfValue = "true") }
  }

  private def testBothModesViaConf(testName: String)(f: String => Unit): Unit = {
    def runTest(autoCompactConfValue: String): Unit = {
      withTempDir { dir =>
        withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> s"$autoCompactConfValue",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
          f(dir.getCanonicalPath)
        }
      }
    }

    test(s"auto-compact-enabled-conf: $testName") { runTest(autoCompactConfValue = "true") }
  }

  private def checkAutoOptimizeLogging(f: => Unit): Boolean = {
    val logs = Log4jUsageLogger.track {
      f
    }
    logs.exists(_.opType.map(_.typeName) === Some("delta.commit.hooks.autoOptimize"))
  }

  import testImplicits._

  test("auto compact event log: inline AC") {
    withTempDir { dir =>
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> s"true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "30") {
        val path = dir.getCanonicalPath
        // Append 1 file to each partition: record skipInsufficientFilesInModifiedPartitions event,
        // as not enough small files.
        var usageLogs = captureOptimizeLogs(AutoCompact.OP_TYPE) {
          createFilesToPartitions(numFilePartitions = 3, numFilesPerPartition = 1, path)
        }
        var log = JsonUtils.mapper.readValue[Map[String, String]](usageLogs.head.blob)
        assert(log("status") == "skipInsufficientFilesInModifiedPartitions")
        // Append 10 more file to each partition: record skipInsufficientFilesInModifiedPartitions
        // event.
        usageLogs = captureOptimizeLogs(AutoCompact.OP_TYPE) {
          createFilesToPartitions(numFilePartitions = 3, numFilesPerPartition = 10, path)
        }
        log = JsonUtils.mapper.readValue[Map[String, String]](usageLogs.head.blob)
        assert(log("status") == "skipInsufficientFilesInModifiedPartitions")
        // Append 20 more files to each partition: record runOnModifiedPartitions on all 3
        // partitions.
        usageLogs = captureOptimizeLogs(AutoCompact.OP_TYPE) {
          createFilesToPartitions(numFilePartitions = 3, numFilesPerPartition = 20, path)
        }
        log = JsonUtils.mapper.readValue[Map[String, String]](usageLogs.head.blob)
        assert(log("status") == "runOnModifiedPartitions" && log("partitions") == "3")
        // Append 30 more file to each partition and check OptimizeMetrics.
        usageLogs = captureOptimizeLogs(metrics = s"${AutoCompact.OP_TYPE}.execute.metrics") {
          createFilesToPartitions(numFilePartitions = 3, numFilesPerPartition = 30, path)
        }
        val metricsLog = JsonUtils.mapper.readValue[OptimizeMetrics](usageLogs.head.blob)
        assert(metricsLog.numBytesSkippedToReduceWriteAmplification === 0)
        assert(metricsLog.numFilesSkippedToReduceWriteAmplification === 0)
        assert(metricsLog.totalConsideredFiles === 93)
        assert(metricsLog.numFilesAdded == 3)
        assert(metricsLog.numFilesRemoved == 93)
        assert(metricsLog.numBins === 3)
      }
    }
  }

  /**
   * Writes `df` twice to the same location and checks that
   *   1. There is only one resultant file.
   *   2. The result is equal to `df` unioned with itself.
   */
  private def checkAutoCompactionWorks(dir: String, df: DataFrame): Unit = {
    df.write.format("delta").mode("append").save(dir)
    val deltaLog = DeltaLog.forTable(spark, dir)
    val newSnapshot = deltaLog.update()
    assert(newSnapshot.version === 1) // 0 is the first commit, 1 is optimize
    assert(deltaLog.update().numOfFiles === 1)

    val isLogged = checkAutoOptimizeLogging {
      df.write.format("delta").mode("append").save(dir)
    }

    assert(isLogged)
    val lastEvent = deltaLog.history.getHistory(Some(1)).head
    assert(lastEvent.operation === "OPTIMIZE")
    assert(lastEvent.operationParameters("auto") === "true")

    assert(deltaLog.update().numOfFiles === 1, "Files should be optimized into a single one")
    checkAnswer(
      df.union(df).toDF(),
      spark.read.format("delta").load(dir)
    )
  }

  testBothModesViaProperty("auto compact should kick in when enabled - table config") { dir =>
    checkAutoCompactionWorks(dir, spark.range(10).toDF("id"))
  }

  testBothModesViaConf("auto compact should kick in when enabled - session config") { dir =>
    checkAutoCompactionWorks(dir, spark.range(10).toDF("id"))
  }

  test("variant auto compact kicks in when enabled - table config") {
    withTempDir { dir =>
      withSQLConf(
          "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact" -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MODIFIED_PARTITIONS_ONLY_ENABLED.key -> "false") {
        checkAutoCompactionWorks(
          dir.getCanonicalPath, spark.range(10).selectExpr("parse_json(cast(id as string)) as v"))
      }
    }
  }

  test("variant auto compact kicks in when enabled - session config") {
    withTempDir { dir =>
      withSQLConf(
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
        checkAutoCompactionWorks(
          dir.getCanonicalPath, spark.range(10).selectExpr("parse_json(cast(id as string)) as v"))
      }
    }
  }

  testBothModesViaProperty("auto compact should not kick in when session config is off") { dir =>
    withSQLConf(DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "false") {
      val isLogged = checkAutoOptimizeLogging {
        spark.range(10).write.format("delta").mode("append").save(dir)
      }

      val deltaLog = DeltaLog.forTable(spark, dir)
      val newSnapshot = deltaLog.update()
      assert(newSnapshot.version === 0) // 0 is the first commit
      assert(deltaLog.update().numOfFiles > 1)
      assert(!isLogged)
    }
  }

  test("auto compact should not kick in after optimize") {
    withTempDir { tempDir =>
        val dir = tempDir.getCanonicalPath
        spark.range(0, 12, 1, 4).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        val newSnapshot = deltaLog.update()
        assert(newSnapshot.version === 0)
        assert(deltaLog.update().numOfFiles === 4)
        spark.sql(s"ALTER TABLE delta.`${tempDir.getCanonicalPath}` SET TBLPROPERTIES " +
          "(delta.autoOptimize.autoCompact = true)")

        val isLogged = checkAutoOptimizeLogging {
          sql(s"optimize delta.`$dir`")
        }

        assert(!isLogged)
        val lastEvent = deltaLog.history.getHistory(Some(1)).head
        assert(lastEvent.operation === "OPTIMIZE")
        assert(lastEvent.operationParameters("auto") === "false")
    }
  }

  testBothModesViaProperty("auto compact should not kick in when there aren't " +
    "enough small files") { dir =>
    withSQLConf(
      DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "6",
      DeltaSQLConf.DELTA_AUTO_COMPACT_MAX_FILE_SIZE.key -> "20000"
    ) {
      AutoCompactPartitionStats.instance(spark).resetTestOnly()

      // First write - 4 small files
      spark.range(10).repartition(4).write.format("delta").mode("append").save(dir)
      val deltaLog = DeltaLog.forTable(spark, dir)
      val newSnapshot = deltaLog.update()
      assert(newSnapshot.version === 0)
      assert(deltaLog.update().numOfFiles === 4, "Should have 4 initial small files")

      // Second write - 4 large files
      spark.range(10000).repartition(4).write.format("delta").mode("append").save(dir)

      val writeEvent = deltaLog.history.getHistory(Some(1)).head
      assert(writeEvent.operation === "WRITE",
        "Large files shouldn't trigger auto compaction")
      assert(deltaLog.update().numOfFiles === 8,
        "Should have 4 small + 4 large files")

      // Third write - 2 more small files to reach minNumFiles
      val isLogged2 = checkAutoOptimizeLogging {
        spark.range(10).repartition(2).write.format("delta").mode("append").save(dir)
      }
      assert(isLogged2)
      val compactionEvent = deltaLog.history.getHistory(Some(3)).head
      assert(compactionEvent.operation === "OPTIMIZE",
        "Should trigger compaction with 6 small files")
      assert(compactionEvent.operationParameters("auto") === "true")

      val finalSnapshot = deltaLog.update()
      assert(finalSnapshot.numOfFiles === 5,
        "Should have 4 large files + 1 compacted small file")

      checkAnswer(
        spark.read.format("delta").load(dir),
        spark.range(10)
          .union(spark.range(10000))
          .union(spark.range(10))
          .toDF()
      )
    }
  }

  testBothModesViaProperty("ensure no NPE in auto compact UDF with null " +
    "partition values") { dir =>
      Seq(null, "", " ").zipWithIndex.foreach { case (partValue, i) =>
        val path = new File(dir, i.toString).getCanonicalPath
        val df1 = spark.range(5).withColumn("part", lit(partValue))
        val df2 = spark.range(5, 10).withColumn("part", lit("1"))
        val isLogged = checkAutoOptimizeLogging {
          // repartition to increase number of files written
          df1.union(df2).repartition(4)
            .write.format("delta").partitionBy("part").mode("append").save(path)
        }
        val deltaLog = DeltaLog.forTable(spark, path)
        val newSnapshot = deltaLog.update()
        assert(newSnapshot.version === 1) // 0 is the first commit, 1 and 2 are optimizes
        assert(newSnapshot.numOfFiles === 2)

        assert(isLogged)
        val lastEvent = deltaLog.history.getHistory(Some(1)).head
        assert(lastEvent.operation === "OPTIMIZE")
        assert(lastEvent.operationParameters("auto") === "true")
      }
  }

  testBothModesViaProperty("check auto compact recorded metrics") { dir =>
    val logs = Log4jUsageLogger.track {
      spark.range(30).repartition(3).write.format("delta").save(dir)
    }
    val metrics = JsonUtils.mapper.readValue[OptimizeMetrics](logs.filter(
      _.tags.get("opType") == Some(s"${AutoCompact.OP_TYPE}.execute.metrics")).head.blob)

    assert(metrics.numFilesRemoved == 3)
    assert(metrics.numFilesAdded == 1)
  }

  private def setTableProperty(log: DeltaLog, key: String, value: String): Unit = {
    spark.sql(s"ALTER TABLE delta.`${log.dataPath}` SET TBLPROPERTIES " +
      s"($key = $value)")
  }

  // ---- Histogram-backed eligibility regression tests ----

  test("auto compact - histogram gate fires on un-partitioned cold start when " +
    "JVM cache is empty") {
    withSQLConf(
        DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_AUTO_COMPACT_USE_FILE_SIZE_HISTOGRAM.key -> "true",
        DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "5",
        DeltaSQLConf.DELTA_AUTO_COMPACT_MAX_FILE_SIZE.key -> "20000",
        DeltaSQLConf.DELTA_FILE_SIZE_HISTOGRAM_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        val dir = tempDir.getCanonicalPath
        // Seed 4 small files (below MIN_NUM_FILES so AC does not fire yet).
        spark.range(10).repartition(4).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        assert(deltaLog.update().numOfFiles === 4)

        // Simulate JVM restart / LRU eviction: clear in-memory per-partition stats.
        AutoCompactPartitionStats.instance(spark).resetTestOnly()

        // Append a single small file. The legacy in-memory cache only sees 1 small file
        // in this commit (< MIN_NUM_FILES=5) and would skip; the snapshot histogram
        // reports 5 small files table-wide and must drive AC to fire.
        val usageLogs = captureOptimizeLogs(AutoCompact.OP_TYPE) {
          spark.range(1).repartition(1).write.format("delta").mode("append").save(dir)
        }
        val log = JsonUtils.mapper.readValue[Map[String, String]](usageLogs.head.blob)
        assert(log("status") === "runOnModifiedPartitionsFromHistogram",
          s"Expected histogram gate to fire AC on cold start; got status=${log("status")}")
        assert(deltaLog.update().numOfFiles === 1,
          "AC should have compacted the 5 small files into 1")
      }
    }
  }

  test("auto compact - feature flag off falls back to in-memory cache " +
    "(no cold-start fire)") {
    withSQLConf(
        DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_AUTO_COMPACT_USE_FILE_SIZE_HISTOGRAM.key -> "false",
        DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "5",
        DeltaSQLConf.DELTA_AUTO_COMPACT_MAX_FILE_SIZE.key -> "20000") {
      withTempDir { tempDir =>
        val dir = tempDir.getCanonicalPath
        spark.range(10).repartition(4).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        assert(deltaLog.update().numOfFiles === 4)

        AutoCompactPartitionStats.instance(spark).resetTestOnly()

        val usageLogs = captureOptimizeLogs(AutoCompact.OP_TYPE) {
          spark.range(1).repartition(1).write.format("delta").mode("append").save(dir)
        }
        val log = JsonUtils.mapper.readValue[Map[String, String]](usageLogs.head.blob)
        assert(log("status") === "skipInsufficientFilesInModifiedPartitions",
          s"Expected legacy cache fall-through to skip AC; got status=${log("status")}")
        assert(deltaLog.update().numOfFiles === 5,
          "AC should NOT have fired with feature flag off; 4 seeded + 1 new = 5 files")
      }
    }
  }

  test("auto compact - histogram absent (legacy snapshot) falls back to in-memory cache") {
    // With the histogram capture feature disabled, postCommitSnapshot.fileSizeHistogram
    // is None even for fresh tables. The eligibility gate must fall through to the
    // existing JVM-local cache logic with no regression.
    withSQLConf(
        DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_AUTO_COMPACT_USE_FILE_SIZE_HISTOGRAM.key -> "true",
        DeltaSQLConf.DELTA_FILE_SIZE_HISTOGRAM_ENABLED.key -> "false",
        DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "5",
        DeltaSQLConf.DELTA_AUTO_COMPACT_MAX_FILE_SIZE.key -> "20000") {
      withTempDir { tempDir =>
        val dir = tempDir.getCanonicalPath
        spark.range(10).repartition(4).write.format("delta").mode("append").save(dir)
        val deltaLog = DeltaLog.forTable(spark, dir)
        AutoCompactPartitionStats.instance(spark).resetTestOnly()

        val usageLogs = captureOptimizeLogs(AutoCompact.OP_TYPE) {
          spark.range(1).repartition(1).write.format("delta").mode("append").save(dir)
        }
        val log = JsonUtils.mapper.readValue[Map[String, String]](usageLogs.head.blob)
        // No "FromHistogram" suffix; legacy path drives the decision.
        assert(!log("status").endsWith("FromHistogram"),
          s"Histogram absent should fall through to legacy path; got status=${log("status")}")
        assert(log("status") === "skipInsufficientFilesInModifiedPartitions")
      }
    }
  }

  test("auto compact - partitioned tables do not consult snapshot histogram") {
    // The histogram is table-wide. For partitioned tables this would over-trigger on
    // the N-partitions x 1-small-file pathology. The eligibility path must skip the
    // histogram branch and rely on the per-partition cache.
    withSQLConf(
        DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_AUTO_COMPACT_USE_FILE_SIZE_HISTOGRAM.key -> "true",
        DeltaSQLConf.DELTA_FILE_SIZE_HISTOGRAM_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "5") {
      withTempDir { tempDir =>
        val dir = tempDir.getCanonicalPath
        // 3 partitions x 2 small files each = 6 files table-wide (> MIN_NUM_FILES=5);
        // no individual partition has >=5, so per-partition selector should skip.
        createFilesToPartitions(numFilePartitions = 3, numFilesPerPartition = 2, dir)
        AutoCompactPartitionStats.instance(spark).resetTestOnly()

        val usageLogs = captureOptimizeLogs(AutoCompact.OP_TYPE) {
          createFilesToPartitions(numFilePartitions = 1, numFilesPerPartition = 1, dir)
        }
        val log = JsonUtils.mapper.readValue[Map[String, String]](usageLogs.head.blob)
        assert(!log("status").endsWith("FromHistogram"),
          s"Partitioned tables must not consult the histogram; got status=${log("status")}")
      }
    }
  }
}

class AutoCompactConfigurationIdColumnMappingSuite extends AutoCompactConfigurationSuite
  with DeltaColumnMappingEnableIdMode {
  override def runAllTests: Boolean = true
}

class AutoCompactExecutionIdColumnMappingSuite extends AutoCompactExecutionSuite
  with DeltaColumnMappingEnableIdMode {
  override def runAllTests: Boolean = true
}

class AutoCompactConfigurationNameColumnMappingSuite extends AutoCompactConfigurationSuite
  with DeltaColumnMappingEnableNameMode {
  override def runAllTests: Boolean = true
}

class AutoCompactExecutionNameColumnMappingSuite extends AutoCompactExecutionSuite
  with DeltaColumnMappingEnableNameMode {
  override def runAllTests: Boolean = true
}

