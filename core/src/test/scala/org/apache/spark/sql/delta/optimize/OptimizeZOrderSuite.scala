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

package org.apache.spark.sql.delta.optimize

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.sources.DeltaSQLConf._
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, TestsStatistics}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.{col, floor, lit, max, struct}
import org.apache.spark.sql.test.SharedSparkSession

trait OptimizePartitionTableHelper extends QueryTest {
  def testPartition(str: String)(testFun: => Any): Unit = {
    test("partitioned table - " + str) {
      testFun
    }
  }
}

/** Tests for Optimize Z-Order by */
trait OptimizeZOrderSuiteBase extends OptimizePartitionTableHelper
  with TestsStatistics
  with SharedSparkSession
  with DeltaColumnMappingTestUtils {
  import testImplicits._


  def executeOptimizeTable(table: String, zOrderBy: Seq[String],
    condition: Option[String] = None): DataFrame
  def executeOptimizePath(path: String, zOrderBy: Seq[String],
    condition: Option[String] = None): DataFrame

  test("optimize command: checks existence of interleaving columns") {
    withTempDir { tempDir =>
      Seq(1, 2, 3).toDF("value")
        .select('value, 'value % 2 as 'id, 'value % 3 as 'id2)
        .write
        .format("delta")
        .save(tempDir.toString)
      val e = intercept[IllegalArgumentException] {
        executeOptimizePath(tempDir.getCanonicalPath, Seq("id", "id3"))
      }
      assert(Seq("id3", "data schema").forall(e.getMessage.contains))
    }
  }

  test("optimize command: interleaving columns can't be partitioning columns") {
    withTempDir { tempDir =>
      Seq(1, 2, 3).toDF("value")
        .select('value, 'value % 2 as 'id, 'value % 3 as 'id2)
        .write
        .format("delta")
        .partitionBy("id")
        .save(tempDir.toString)
      val e = intercept[IllegalArgumentException] {
        executeOptimizePath(tempDir.getCanonicalPath, Seq("id", "id2"))
      }
      assert(e.getMessage === DeltaErrors.zOrderingOnPartitionColumnException("id").getMessage)
    }
  }

  test("optimize command: interleaving with nested columns") {
    withTempDir { tempDir =>
      val df = spark.read.json(Seq("""{"a":1,"b":{"c":2,"d":3}}""").toDS())
      df.write.format("delta").save(tempDir.toString)
      executeOptimizePath(tempDir.getCanonicalPath, Seq("a", "b.c"))
    }
  }

  testPartition("optimize on null partition column") {
    withTempDir { tempDir =>
      (1 to 5).foreach { _ =>
        Seq(("a", 1), ("b", 2), (null.asInstanceOf[String], 3), ("", 4)).toDF("part", "value")
          .write
          .partitionBy("part")
          .format("delta")
          .mode("append")
          .save(tempDir.getAbsolutePath)
      }

      var df = spark.read.format("delta").load(tempDir.getAbsolutePath)
      val deltaLog = loadDeltaLog(tempDir.getAbsolutePath)
      val part = "part".phy(deltaLog)
      var preOptInputFiles = groupInputFilesByPartition(df.inputFiles, deltaLog)
      assert(preOptInputFiles.forall(_._2.length > 1))
      assert(preOptInputFiles.keys.exists(_ == (part, nullPartitionValue)))

      executeOptimizePath(tempDir.getAbsolutePath, Seq("value"))

      df = spark.read.format("delta").load(tempDir.getAbsolutePath)
      preOptInputFiles = groupInputFilesByPartition(df.inputFiles, deltaLog)
      assert(preOptInputFiles.forall(_._2.length == 1))
      assert(preOptInputFiles.keys.exists(_ == (part, nullPartitionValue)))

      checkAnswer(
        df.groupBy('part).count(),
        Seq(Row("a", 5), Row("b", 5), Row(null, 10))
      )
    }
  }

  test("optimize: Zorder on col name containing dot") {
    withTempDir { tempDir =>
        (0.to(79).seq ++ 40.to(79).seq ++ 60.to(79).seq ++ 70.to(79).seq ++ 75.to(79).seq)
          .toDF("id")
          .withColumn("flat.a", $"id" + 1)
          .write
          .format("delta")
          .save(tempDir.toString)

        val deltaLog = DeltaLog.forTable(spark, tempDir)
        val numFilesBefore = deltaLog.snapshot.numOfFiles
        val res = executeOptimizePath(tempDir.getCanonicalPath, Seq("`flat.a`"))
        val metrics = res.select($"metrics.*").as[OptimizeMetrics].head()
        val numFilesAfter = deltaLog.snapshot.numOfFiles
        assert(metrics.numFilesAdded === numFilesAfter)
        assert(metrics.numFilesRemoved === numFilesBefore)
    }
  }

  test("optimize: Zorder on a nested column") {
    withTempDir { tempDir =>
        (0.to(79).seq ++ 40.to(79).seq ++ 60.to(79).seq ++ 70.to(79).seq ++ 75.to(79).seq)
          .toDF("id")
          .withColumn("nested", struct(struct('id + 2 as 'b, 'id + 3 as 'c) as 'sub))
          .write
          .format("delta")
          .save(tempDir.toString)

        val deltaLog = DeltaLog.forTable(spark, tempDir)
        val numFilesBefore = deltaLog.snapshot.numOfFiles
        val res = executeOptimizePath(tempDir.getCanonicalPath, Seq("nested.sub.c"))
        val metrics = res.select($"metrics.*").as[OptimizeMetrics].head()
        val numFilesAfter = deltaLog.snapshot.numOfFiles
        assert(metrics.numFilesAdded === numFilesAfter)
        assert(metrics.numFilesRemoved === numFilesBefore)
    }
  }

  test("optimize: ZOrder on a column without stats") {
    withTempDir { tempDir =>
      withSQLConf("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols" ->
        "1", DELTA_OPTIMIZE_ZORDER_COL_STAT_CHECK.key -> "true") {
        val data = Seq(1, 2, 3).toDF("id")
        data.withColumn("nested",
          struct(struct('id + 1 as 'p1, 'id + 2 as 'p2) as 'a, 'id + 3 as 'b))
          .write
          .format("delta")
          .save(tempDir.getAbsolutePath)
        val e1 = intercept[AnalysisException] {
          executeOptimizeTable(s"delta.`${tempDir.getPath}`", Seq("nested.b"))
        }
        assert(e1.getMessage == DeltaErrors
          .zOrderingOnColumnWithNoStatsException(Seq[String]("nested.b"), spark)
          .getMessage)
        val e2 = intercept[AnalysisException] {
          executeOptimizeTable(s"delta.`${tempDir.getPath}`", Seq("nested.a.p1"))
        }
        assert(e2.getMessage == DeltaErrors
          .zOrderingOnColumnWithNoStatsException(Seq[String]("nested.a.p1"), spark)
          .getMessage)
        val e3 = intercept[AnalysisException] {
          executeOptimizeTable(s"delta.`${tempDir.getPath}`",
            Seq("nested.a.p1", "nested.b"))
        }
        assert(e3.getMessage == DeltaErrors
          .zOrderingOnColumnWithNoStatsException(
            Seq[String]("nested.a.p1", "nested.b"), spark)
          .getMessage)
      }
    }
  }

  statsTest("optimize command: interleaving") {
    def statsDF(deltaLog: DeltaLog): DataFrame = {
      val (c1, c2, c3) = ("c1".phy(deltaLog), "c2".phy(deltaLog), "c3".phy(deltaLog))
      getStatsDf(deltaLog, Seq(
        $"numRecords",
        struct($"minValues.`$c1`", $"minValues.`$c2`", $"minValues.`$c3`"),
        struct($"maxValues.`$c1`", $"maxValues.`$c2`", $"maxValues.`$c3`")))
    }

    withTempDir { tempDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

      {
        val df = spark.range(100)
            .map(i => (i, 99 - i, (i + 50) % 100))
            .toDF("c1", "c2", "c3")

        df.repartitionByRange(4, $"c1", $"c2", $"c3")
            .write
            .format("delta")
            .save(tempDir.toString)
      }
      assert(deltaLog.snapshot.allFiles.count() == 4)
      checkAnswer(statsDF(deltaLog), Seq(
        Row(25, Row(0, 75, 50), Row(24, 99, 74)),
        Row(25, Row(25, 50, 75), Row(49, 74, 99)),
        Row(25, Row(50, 25, 0), Row(74, 49, 24)),
        Row(25, Row(75, 0, 25), Row(99, 24, 49))))

      withSQLConf(
        DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> "1000000"
      ) {
        val res = executeOptimizePath(tempDir.getCanonicalPath, Seq("c1", "c2", "c3"))
        val metrics = res.select($"metrics.*").as[OptimizeMetrics].head()
        assert(metrics.zOrderStats.get.mergedFiles.num == 4)
        assert(deltaLog.snapshot.allFiles.count() == 1)
        checkAnswer(statsDF(deltaLog),
                    Row(100, Row(0, 0, 0), Row(99, 99, 99)))
      }

      // I want to get 4 files again, in order for this to be comparable to the initial scenario
      val maxFileSize = deltaLog.snapshot.allFiles.head().size / 4
      withSQLConf(
        DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> maxFileSize.toString
      ) {
        val res = executeOptimizePath(tempDir.getCanonicalPath, Seq("c1", "c2", "c3"))
        val metrics = res.select($"metrics.*").as[OptimizeMetrics].head()
        val expectedFileCount = 4
        val expectedStats: Seq[Row] = Seq(
          Row(25, Row(0, 50, 50), Row(49, 99, 99)),
          Row(25, Row(16, 20, 18), Row(79, 83, 85)),
          Row(25, Row(36, 36, 0), Row(63, 63, 96)),
          Row(25, Row(64, 0, 14), Row(99, 35, 49)))
        assert(metrics.zOrderStats.get.mergedFiles.num == 1)
        assert(deltaLog.snapshot.allFiles.count() == expectedFileCount)
        checkAnswer(statsDF(deltaLog), expectedStats)
      }
    }
  }
}

/**
 * Runs optimize compaction tests using OPTIMIZE SQL
 */
class OptimizeZOrderSQLSuite extends OptimizeZOrderSuiteBase
  with DeltaSQLCommandTest {
  import testImplicits._

  def executeOptimizeTable(table: String, zOrderBy: Seq[String],
      condition: Option[String] = None): DataFrame = {
    val conditionClause = condition.map(c => s"WHERE $c").getOrElse("")
    val zOrderClause = s"ZORDER BY (${zOrderBy.mkString(", ")})"
    spark.sql(s"OPTIMIZE $table $conditionClause $zOrderClause")
  }

  def executeOptimizePath(path: String, zOrderBy: Seq[String],
      condition: Option[String] = None): DataFrame = {
    executeOptimizeTable(s"'$path'", zOrderBy, condition)
  }

  test("optimize command: no need for parenthesis") {
    withTempDir { tempDir =>
      val df = spark.read.json(Seq("""{"a":1,"b":{"c":2,"d":3}}""").toDS())
      df.write.format("delta").save(tempDir.toString)
      spark.sql(s"OPTIMIZE '${tempDir.getCanonicalPath}' ZORDER BY a, b.c")
    }
  }
}

/**
 * Runs optimize compaction tests using OPTIMIZE Scala APIs
 */
class OptimizeZOrderScalaSuite extends OptimizeZOrderSuiteBase
    with DeltaSQLCommandTest {


  def executeOptimizeTable(table: String, zOrderBy: Seq[String],
      condition: Option[String] = None): DataFrame = {
    if (condition.isDefined) {
      DeltaTable.forName(table).optimize().where(condition.get).executeZOrderBy(zOrderBy: _*)
    } else {
      DeltaTable.forName(table).optimize().executeZOrderBy(zOrderBy: _*)
    }
  }

  def executeOptimizePath(path: String, zOrderBy: Seq[String],
      condition: Option[String] = None): DataFrame = {
    if (condition.isDefined) {
      DeltaTable.forPath(path).optimize().where(condition.get).executeZOrderBy(zOrderBy: _*)
    } else {
      DeltaTable.forPath(path).optimize().executeZOrderBy(zOrderBy: _*)
    }
  }
}

class OptimizeZOrderNameColumnMappingSuite extends OptimizeZOrderSQLSuite
  with DeltaColumnMappingEnableNameMode
  with DeltaColumnMappingTestUtils

class OptimizeZOrderIdColumnMappingSuite extends OptimizeZOrderSQLSuite
  with DeltaColumnMappingEnableIdMode
  with DeltaColumnMappingTestUtils
