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
import org.apache.spark.sql.delta.DeltaExcludedBySparkVersionTestMixinShims
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

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Literal
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
 * It also tests any AutoCompaction specific behavior.
 */
class AutoCompactSuite extends
    CompactionTestHelperForAutoCompaction
  with DeltaSQLCommandTest
  with SharedSparkSession
  with AutoCompactTestUtils
  with DeltaExcludedBySparkVersionTestMixinShims {

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
        // Append 1 file to each partition: record runOnModifiedPartitions event, as is first write
        var usageLogs = captureOptimizeLogs(AutoCompact.OP_TYPE) {
          createFilesToPartitions(numFilePartitions = 3, numFilesPerPartition = 1, path)
        }
        var log = JsonUtils.mapper.readValue[Map[String, String]](usageLogs.head.blob)
        assert(log("status") == "runOnModifiedPartitions" && log("partitions") == "3")
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

  testSparkMasterOnly("variant auto compact kicks in when enabled - table config") {
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

  testSparkMasterOnly("variant auto compact kicks in when enabled - session config") {
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
    "enough files") { dir =>
    withSQLConf(DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "5") {
      AutoCompactPartitionStats.instance(spark).resetTestOnly()
      spark.range(10).repartition(4).write.format("delta").mode("append").save(dir)

      val deltaLog = DeltaLog.forTable(spark, dir)
      val newSnapshot = deltaLog.update()
      assert(newSnapshot.version === 0)
      assert(deltaLog.update().numOfFiles === 4)

      val isLogged2 = checkAutoOptimizeLogging {
        spark.range(10).repartition(4).write.format("delta").mode("append").save(dir)
      }

      assert(isLogged2)
      val lastEvent = deltaLog.history.getHistory(Some(1)).head
      assert(lastEvent.operation === "OPTIMIZE")
      assert(lastEvent.operationParameters("auto") === "true")

      assert(deltaLog.update().numOfFiles === 1, "Files should be optimized into a single one")

      checkAnswer(
        spark.read.format("delta").load(dir),
        spark.range(10).union(spark.range(10)).toDF()
      )
    }
  }

  testBothModesViaProperty("ensure no NPE in auto compact UDF with null " +
    "partition values") { dir =>
      Seq(null, "", " ").map(UTF8String.fromString).zipWithIndex.foreach { case (partValue, i) =>
        val path = new File(dir, i.toString).getCanonicalPath
        val df1 = spark.range(5).withColumn("part", new Column(Literal(partValue, StringType)))
        val df2 = spark.range(5, 10).withColumn("part", new Column(Literal("1")))
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
}

class AutoCompactIdColumnMappingSuite extends AutoCompactSuite
  with DeltaColumnMappingEnableIdMode {
  override def runAllTests: Boolean = true
}

class AutoCompactNameColumnMappingSuite extends AutoCompactSuite
  with DeltaColumnMappingEnableNameMode {
  override def runAllTests: Boolean = true
}

