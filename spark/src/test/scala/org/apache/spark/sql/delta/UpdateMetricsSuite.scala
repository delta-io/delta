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

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.DatabricksLogging
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{Dataset, QueryTest}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for metrics of Delta UPDATE command.
 */
class UpdateMetricsSuite extends QueryTest
  with SharedSparkSession
  with DatabricksLogging  with DeltaSQLCommandTest {


  /**
   * Case class to parameterize tests.
   */
  case class TestConfiguration(
      partitioned: Boolean,
      cdfEnabled: Boolean
  )

  /**
   * Case class to parameterize metric results.
   */
  case class TestMetricResults(
      operationMetrics: Map[String, Long]
  )

  /**
   * Helper to generate tests for all configuration parameters.
   */
  protected def testUpdateMetrics(name: String)(testFn: TestConfiguration => Unit): Unit = {
    for {
      partitioned <- BOOLEAN_DOMAIN
      cdfEnabled <- Seq(false)
    } {
      val testConfig =
        TestConfiguration(partitioned = partitioned,
          cdfEnabled = cdfEnabled
        )
      var testName =
        s"update-metrics: $name - Partitioned = $partitioned, cdfEnabled = $cdfEnabled"
      test(testName) {
        testFn(testConfig)
      }
    }
  }


  /**
   * Create a table from the provided dataset.
   *
   * If an partitioned table is needed, then we create one data partition per Spark partition,
   * i.e. every data partition will contain one file.
   *
   * Also an extra column is added to be used in non-partition filters.
   */
  protected def createTempTable(
      table: Dataset[_],
      tableName: String,
      testConfig: TestConfiguration): Unit = {
    val numRows = table.count()
    val numPartitions = table.rdd.getNumPartitions
    val numRowsPerPart = if (numRows > 0 && numPartitions < numRows) {
      numRows / numPartitions
    } else {
      1
    }
    val partitionBy = if (testConfig.partitioned) {
      Seq("partCol")
    } else {
      Seq()
    }
    table.withColumn("partCol", expr(s"floor(id / $numRowsPerPart)"))
      .withColumn("extraCol", expr(s"$numRows - id"))
      .write
      .partitionBy(partitionBy: _*)
      .format("delta")
      .saveAsTable(tableName)
  }

  /**
   * Run an update command and capture operation metrics from Delta log.
   *
   */
  private def runUpdateAndCaptureMetrics(
      table: Dataset[_],
      where: String,
      testConfig: TestConfiguration): TestMetricResults = {
    val tableName = "target"
    val whereClause = if (where.nonEmpty) {
      s"WHERE $where"
    } else {
      ""
    }
    var operationMetrics: Map[String, Long] = null
    import testImplicits._
    withSQLConf(
      DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_SKIP_RECORDING_EMPTY_COMMITS.key -> "false",
      DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> testConfig.cdfEnabled.toString) {
      withTable(tableName) {
        createTempTable(table, tableName, testConfig)
          val resultDf = spark.sql(s"UPDATE $tableName SET id = -1 $whereClause")
        operationMetrics = DeltaMetricsUtils.getLastOperationMetrics(tableName)
      }
    }
    TestMetricResults(
      operationMetrics
    )
  }

  /**
   * Run a update command and check all available metrics.
   * We allow some metrics to be missing, by setting their value to -1.
   */
  private def runUpdateAndCheckMetrics(
      table: Dataset[_],
      where: String,
      expectedOperationMetrics: Map[String, Long],
      testConfig: TestConfiguration): Unit = {
    // Run the update capture and get all metrics.
    val results = runUpdateAndCaptureMetrics(table, where, testConfig)

    // Check operation metrics schema.
    val unknownKeys = results.operationMetrics.keySet -- DeltaOperationMetrics.UPDATE --
      DeltaOperationMetrics.WRITE
    assert(unknownKeys.isEmpty,
      s"Unknown operation metrics for UPDATE command: ${unknownKeys.mkString(", ")}")

    // Check values of expected operation metrics. For all unspecified deterministic metrics,
    // we implicitly expect a zero value.
    val requiredMetrics = Set(
      "numCopiedRows",
      "numUpdatedRows",
      "numAddedFiles",
      "numRemovedFiles",
      "numAddedChangeFiles")
    val expectedMetricsWithDefaults =
      requiredMetrics.map(k => k -> 0L).toMap ++ expectedOperationMetrics
    val expectedMetricsFiltered = expectedMetricsWithDefaults.filter(_._2 >= 0)
    DeltaMetricsUtils.checkOperationMetrics(
      expectedMetrics = expectedMetricsFiltered,
      operationMetrics = results.operationMetrics)


    // Check time operation metrics.
    val expectedTimeMetrics =
      Set("scanTimeMs", "rewriteTimeMs", "executionTimeMs").filter(
        k => expectedOperationMetrics.get(k).forall(_ >= 0)
      )
    DeltaMetricsUtils.checkOperationTimeMetrics(
      operationMetrics = results.operationMetrics,
      expectedMetrics = expectedTimeMetrics)
  }


  for (whereClause <- Seq("", "1 = 1")) {
    testUpdateMetrics(s"update all with where = '$whereClause'") { testConfig =>
      val numFiles = 5
      val numRows = 100
      val numAddedChangeFiles = if (testConfig.partitioned && testConfig.cdfEnabled) {
        5
      } else if (testConfig.cdfEnabled) {
        2
      } else {
        0
      }
      runUpdateAndCheckMetrics(
        table = spark.range(start = 0, end = numRows, step = 1, numPartitions = numFiles),
        where = whereClause,
        expectedOperationMetrics = Map(
          "numCopiedRows" -> 0,
          "numUpdatedRows" -> -1,
          "numOutputRows" -> -1,
          "numFiles" -> -1,
          "numAddedFiles" -> -1,
          "numRemovedFiles" -> numFiles,
          "numAddedChangeFiles" -> numAddedChangeFiles
        ),
        testConfig = testConfig
      )
    }
  }

  testUpdateMetrics("update with false predicate") { testConfig =>
    runUpdateAndCheckMetrics(
      table = spark.range(start = 0, end = 100, step = 1, numPartitions = 5),
      where = "1 != 1",
      expectedOperationMetrics = Map(
        "numCopiedRows" -> 0,
        "numUpdatedRows" -> 0,
        "numAddedFiles" -> 0,
        "numRemovedFiles" -> 0,
        "numAddedChangeFiles" -> 0,
        "scanTimeMs" -> -1,
        "rewriteTimeMs" -> -1,
        "executionTimeMs" -> -1
      ),
      testConfig = testConfig
    )
  }

  testUpdateMetrics("update with unsatisfied static predicate") { testConfig =>
    runUpdateAndCheckMetrics(
      table = spark.range(start = 0, end = 100, step = 1, numPartitions = 5),
      where = "id < 0 or id > 100",
      expectedOperationMetrics = Map(
        "numCopiedRows" -> 0,
        "numUpdatedRows" -> 0,
        "numAddedFiles" -> 0,
        "numRemovedFiles" -> 0,
        "numAddedChangeFiles" -> 0,
        "scanTimeMs" -> -1,
        "rewriteTimeMs" -> -1,
        "executionTimeMs" -> -1
      ),
      testConfig = testConfig
    )
  }

  testUpdateMetrics("update with unsatisfied dynamic predicate") { testConfig =>
    runUpdateAndCheckMetrics(
      table = spark.range(start = 0, end = 100, step = 1, numPartitions = 5),
      where = "id / 200 > 1 ",
      expectedOperationMetrics = Map(
        "numCopiedRows" -> 0,
        "numUpdatedRows" -> 0,
        "numAddedFiles" -> 0,
        "numRemovedFiles" -> 0,
        "numAddedChangeFiles" -> 0,
        "scanTimeMs" -> -1,
        "rewriteTimeMs" -> -1,
        "executionTimeMs" -> -1
      ),
      testConfig = testConfig
    )
  }

  for (whereClause <- Seq("id = 0", "id >= 49 and id < 50")) {
    testUpdateMetrics(s"update one row with where = `$whereClause`") { testConfig =>
      var numCopiedRows = 19
      val numAddedFiles = 1
      var numRemovedFiles = 1
      runUpdateAndCheckMetrics(
        table = spark.range(start = 0, end = 100, step = 1, numPartitions = 5),
        where = whereClause,
        expectedOperationMetrics = Map(
          "numCopiedRows" -> numCopiedRows,
          "numUpdatedRows" -> 1,
          "numAddedFiles" -> numAddedFiles,
          "numRemovedFiles" -> numRemovedFiles,
          "numAddedChangeFiles" -> {
            if (testConfig.cdfEnabled) {
              1
            } else {
              0
            }
          }
        ),
        testConfig = testConfig
      )
    }
  }

  testUpdateMetrics("update one file") { testConfig =>
    runUpdateAndCheckMetrics(
      table = spark.range(start = 0, end = 100, step = 1, numPartitions = 5),
      where = "id < 20",
      expectedOperationMetrics = Map(
        "numCopiedRows" -> 0,
        "numUpdatedRows" -> 20,
        "numAddedFiles" -> 1,
        "numRemovedFiles" -> 1,
        "numAddedChangeFiles" -> {
          if (testConfig.cdfEnabled) {
            1
          } else {
            0
          }
        }
      ),
      testConfig = testConfig
    )
  }

  testUpdateMetrics("update one row per file") { testConfig =>
    val numPartitions = 5
    var numCopiedRows = 95
    val numAddedFiles = if (testConfig.partitioned) 5 else 2
    var numRemovedFiles = 5
    var unpartitionedNumAddFiles = 2
    runUpdateAndCheckMetrics(
      table = spark.range(start = 0, end = 100, step = 1, numPartitions = numPartitions),
      where = "id in (5, 25, 45, 65, 85)",
      expectedOperationMetrics = Map(
        "numCopiedRows" -> numCopiedRows,
        "numUpdatedRows" -> 5,
        "numAddedFiles" -> {
          if (testConfig.partitioned) {
            5
          } else {
            unpartitionedNumAddFiles
          }
        },
        "numRemovedFiles" -> numRemovedFiles,
        "numAddedChangeFiles" -> {
          if (testConfig.cdfEnabled) {
            if (testConfig.partitioned) {
              5
            } else {
              unpartitionedNumAddFiles
            }
          } else {
            0
          }
        }
      ),
      testConfig = testConfig
    )
  }

}
