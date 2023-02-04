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
import org.apache.spark.sql.delta.actions.{Action, AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.commands.DeleteMetric
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{Dataset, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for metrics of Delta DELETE command.
 */
class DeleteMetricsSuite extends QueryTest
  with SharedSparkSession
  with DatabricksLogging  with DeltaSQLCommandTest {


  /*
   * Case class to parameterize tests.
   */
  case class TestConfiguration(
      partitioned: Boolean,
      cdfEnabled: Boolean
  )

  case class TestMetricResults(
      expectedNumAddBytes: Long,
      expectedNumRemoveBytes: Long,
      operationMetrics: Map[String, Long],
      numAffectedRows: Long
  )

  /*
   * Helper to generate tests for all configuration parameters.
   */
  protected def testDeleteMetrics(name: String)(testFn: TestConfiguration => Unit): Unit = {
    for {
      partitioned <- BOOLEAN_DOMAIN
      cdfEnabled <- BOOLEAN_DOMAIN
    } {
      val testConfig = TestConfiguration(
        partitioned = partitioned,
        cdfEnabled = cdfEnabled
      )
      var testName =
        s"delete-metrics: $name - Partitioned = $partitioned, cdfEnabled = $cdfEnabled"
      test(testName) {
        testFn(testConfig)
      }
    }
  }

  /*
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
    val numRowsPerPart = if (numRows > 0 && numPartitions < numRows) numRows / numPartitions else 1
    val partitionBy = if (testConfig.partitioned) Seq("partCol") else Seq()
    table.withColumn("partCol", expr(s"floor(id / $numRowsPerPart)"))
      .withColumn("extraCol", expr(s"$numRows - id"))
      .write
      .partitionBy(partitionBy: _*)
      .format("delta")
      .saveAsTable(tableName)
  }

  /*
   * Run a delete command, and capture number of affected rows, operation metrics from Delta
   * log and usage metrics.
   */
  def runDeleteAndCaptureMetrics(
      table: Dataset[_],
      where: String,
      testConfig: TestConfiguration): TestMetricResults = {
    val tableName = "target"
    val whereClause = Option(where).map(c => s"WHERE $c").getOrElse("")
    var numAffectedRows = -1L
    var operationMetrics: Map[String, Long] = null
    var (expectedNumAddedBytes, expectedNumRemovedBytes) = (0L, 0L)
    withSQLConf(
      DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_SKIP_RECORDING_EMPTY_COMMITS.key -> "false",
      DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey ->
        testConfig.cdfEnabled.toString) {
      withTable(tableName) {
        createTempTable(table, tableName, testConfig)

          val resultDf = spark.sql(s"DELETE FROM $tableName $whereClause")
          assert(!resultDf.isEmpty)
          numAffectedRows = resultDf.take(1).head(0).toString.toLong

        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
        val changes = deltaLog.getChanges(deltaLog.update().version).flatMap(_._2).toSeq

        // To get the expected Added and Removed Bytes we need to filter out files
        // that have a dv associated with their path. Since we won't be physically deleting/adding
        // if there is a dv associated.
        val pathsWithDvs = changes.collect {
          case a: AddFile if a.deletionVector != null => a.path
        }.toSet

        expectedNumAddedBytes = changes.collect {
          case a: AddFile if !pathsWithDvs.contains(a.path) => a.size
        }.sum
        expectedNumRemovedBytes = changes.collect {
          case r: RemoveFile if !pathsWithDvs.contains(r.path) => r.getFileSize
        }.sum
        operationMetrics = DeltaMetricsUtils.getLastOperationMetrics(tableName)
      }
    }
    TestMetricResults(
      expectedNumAddedBytes,
      expectedNumRemovedBytes,
      operationMetrics,
      numAffectedRows
    )
  }

  /*
   * Run a delete command and check all available metrics.
   * We allow some metrics to be missing, by setting their value to -1.
   */
  def runDeleteAndCheckMetrics(
    table: Dataset[_],
    where: String,
    expectedNumAffectedRows: Long,
    expectedOperationMetrics: Map[String, Long],
    testConfig: TestConfiguration): Unit = {
    // Run the delete capture and get all metrics.
    val testMetricResults = runDeleteAndCaptureMetrics(table, where, testConfig)
    val operationMetrics = testMetricResults.operationMetrics

    // Check the number of deleted rows.
    assert(testMetricResults.numAffectedRows === expectedNumAffectedRows)

    // Check operation metrics schema.
    val unknownKeys = operationMetrics.keySet -- DeltaOperationMetrics.DELETE --
      DeltaOperationMetrics.WRITE
    assert(unknownKeys.isEmpty,
      s"Unknown operation metrics for DELETE command: ${unknownKeys.mkString(", ")}")

    // Check values of expected operation metrics. For all unspecified deterministic metrics,
    // we implicitly expect a zero value.
    val requiredMetrics = Set(
      "numCopiedRows",
      "numDeletedRows",
      "numAddedFiles",
      "numRemovedFiles",
      "numAddedChangeFiles")
    val expectedMetricsWithDefaults =
      requiredMetrics.map(k => k -> 0L).toMap ++ expectedOperationMetrics
    val expectedMetricsFiltered = expectedMetricsWithDefaults.filter(_._2 >= 0)
    DeltaMetricsUtils.checkOperationMetrics(
      expectedMetrics = expectedMetricsFiltered,
      operationMetrics = operationMetrics)

    // check values byte-level metrics returned
    if (testMetricResults.numAffectedRows > 0) {
      assert(
        operationMetrics("numAddedBytes") == testMetricResults.expectedNumAddBytes,
        s"Expected ${testMetricResults.expectedNumAddBytes} bytes added, but got" +
          s" ${operationMetrics("numAddedBytes")}"
      )
      assert(operationMetrics("numRemovedBytes") == testMetricResults.expectedNumRemoveBytes,
        s"Expected ${testMetricResults.expectedNumRemoveBytes} bytes removed, but got" +
          s" ${operationMetrics("numRemovedBytes")}"
      )
    }


    // Check time operation metrics.
    val expectedTimeMetrics =
    Set("scanTimeMs", "rewriteTimeMs", "executionTimeMs").filter(
      k => expectedOperationMetrics.get(k).forall(_ >= 0)
    )
    DeltaMetricsUtils.checkOperationTimeMetrics(
      operationMetrics = operationMetrics,
      expectedMetrics = expectedTimeMetrics)
  }


  val zeroDeleteMetrics: DeleteMetric = DeleteMetric(
    condition = "",
    numFilesTotal = 0,
    numTouchedFiles = 0,
    numRewrittenFiles = 0,
    numRemovedFiles = 0,
    numAddedFiles = 0,
    numAddedChangeFiles = 0,
    numFilesBeforeSkipping = 0,
    numBytesBeforeSkipping = -1, // We don't want to assert equality on bytes
    numFilesAfterSkipping = 0,
    numBytesAfterSkipping = -1, // We don't want to assert equality on bytes
    numPartitionsAfterSkipping = None,
    numPartitionsAddedTo = None,
    numPartitionsRemovedFrom = None,
    numCopiedRows = None,
    numDeletedRows = None,
    numBytesAdded = -1, // We don't want to assert equality on bytes
    numBytesRemoved = -1, // We don't want to assert equality on bytes
    changeFileBytes = -1, // We don't want to assert equality on bytes
    scanTimeMs = 0,
    rewriteTimeMs = 0
  )


  test("delete along partition boundary") {
    import testImplicits._

    Seq(true, false).foreach { cdfEnabled =>
      Seq(true, false).foreach { deltaCollectStatsEnabled =>
        Seq(true, false).foreach { deltaDmlMetricsFromMetadataEnabled =>
          withSQLConf(
            DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> cdfEnabled.toString,
            DeltaSQLConf.DELTA_COLLECT_STATS.key -> deltaCollectStatsEnabled.toString,
            DeltaSQLConf.DELTA_DML_METRICS_FROM_METADATA.key
              -> deltaDmlMetricsFromMetadataEnabled.toString
          ) {
            withTable("t1") {
              spark.range(100).withColumn("part", 'id % 10).toDF().write
                .partitionBy("part").format("delta").saveAsTable("t1")
              val result = spark.sql("DELETE FROM t1 WHERE part=1")
                .take(1).head(0).toString.toLong
              val opMetrics = DeltaMetricsUtils.getLastOperationMetrics("t1")

              assert(opMetrics("numRemovedFiles") > 0)
              if (deltaCollectStatsEnabled && deltaDmlMetricsFromMetadataEnabled) {
                assert(opMetrics("numDeletedRows") == 10)
                assert(result == 10)
              } else {
                assert(!opMetrics.contains("numDeletedRows"))
                assert(result == -1)
              }
            }
          }
        }
      }
    }
  }

  testDeleteMetrics("delete from empty table") { testConfig =>
    for (where <- Seq("", "1 = 1", "1 != 1", "id > 50")) {
      def executeTest: Unit = runDeleteAndCheckMetrics(
        table = spark.range(0),
        where = where,
        expectedNumAffectedRows = 0,
        expectedOperationMetrics = Map(
          "numCopiedRows" -> 0,
          "numDeletedRows" -> 0,
          "numAddedFiles" -> 0,
          "numRemovedFiles" -> 0,
          "numAddedChangeFiles" -> 0,
          "scanTimeMs" -> -1,
          "rewriteTimeMs" -> -1,
          "executionTimeMs" -> -1
        ),
        testConfig = testConfig
      )

      executeTest
    }
  }

  for (whereClause <- Seq("", "1 = 1")) {
    testDeleteMetrics(s"delete all with where = '$whereClause'") { testConfig =>
      runDeleteAndCheckMetrics(
        table = spark.range(start = 0, end = 100, step = 1, numPartitions = 5),
        where = whereClause,
        expectedNumAffectedRows = 100,
        expectedOperationMetrics = Map(
          "numCopiedRows" -> -1,
          "numDeletedRows" -> 100,
          "numOutputRows" -> -1,
          "numFiles" -> -1,
          "numAddedFiles" -> -1,
          "numRemovedFiles" -> 5,
          "numAddedChangeFiles" -> 0
        ),
        testConfig = testConfig
      )
    }
  }

  testDeleteMetrics("delete with false predicate") { testConfig => {
    runDeleteAndCheckMetrics(
      table = spark.range(start = 0, end = 100, step = 1, numPartitions = 5),
      where = "1 != 1",
      expectedNumAffectedRows = 0L,
      expectedOperationMetrics = Map(
        "numCopiedRows" -> 0,
        "numDeletedRows" -> 0,
        "numAddedFiles" -> 0,
        "numRemovedFiles" -> 0,
        "numAddedChangeFiles" -> 0,
        "scanTimeMs" -> -1,
        "rewriteTimeMs" -> -1,
        "executionTimeMs" -> -1
      ),
      testConfig = testConfig
    )
  }}

  testDeleteMetrics("delete with unsatisfied static predicate") { testConfig => {
    runDeleteAndCheckMetrics(
      table = spark.range(start = 0, end = 100, step = 1, numPartitions = 5),
      where = "id < 0 or id > 100",
      expectedNumAffectedRows = 0L,
      expectedOperationMetrics = Map(
        "numCopiedRows" -> 0,
        "numDeletedRows" -> 0,
        "numAddedFiles" -> 0,
        "numRemovedFiles" -> 0,
        "numAddedChangeFiles" -> 0,
        "scanTimeMs" -> -1,
        "rewriteTimeMs" -> -1,
        "executionTimeMs" -> -1
      ),
      testConfig = testConfig
    )
  }}

  testDeleteMetrics("delete with unsatisfied dynamic predicate") { testConfig => {
    runDeleteAndCheckMetrics(
      table = spark.range(start = 0, end = 100, step = 1, numPartitions = 5),
      where = "id / 200 > 1 ",
      expectedNumAffectedRows = 0L,
      expectedOperationMetrics = Map(
        "numCopiedRows" -> 0,
        "numDeletedRows" -> 0,
        "numAddedFiles" -> 0,
        "numRemovedFiles" -> 0,
        "numAddedChangeFiles" -> 0,
        "scanTimeMs" -> -1,
        "rewriteTimeMs" -> -1,
        "executionTimeMs" -> -1
      ),
      testConfig = testConfig
    )
  }}

  for (whereClause <- Seq("id = 0", "id >= 49 and id < 50")) {
    testDeleteMetrics(s"delete one row with where = `$whereClause`") { testConfig =>
      var numAddedFiles = 1
      var numRemovedFiles = 1
      val numRemovedRows = 1
      var numCopiedRows = 19
      runDeleteAndCheckMetrics(
        table = spark.range(start = 0, end = 100, step = 1, numPartitions = 5),
        where = whereClause,
        expectedNumAffectedRows = 1L,
        expectedOperationMetrics = Map(
          "numCopiedRows" -> numCopiedRows,
          "numDeletedRows" -> numRemovedRows,
          "numAddedFiles" -> numAddedFiles,
          "numRemovedFiles" -> numRemovedFiles,
          "numAddedChangeFiles" -> {
            if (testConfig.cdfEnabled
            ) {
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

  testDeleteMetrics("delete one file") { testConfig =>
    val numRemovedFiles = 1
    val numRemovedRows = 20

    def executeTest: Unit = runDeleteAndCheckMetrics(
      table = spark.range(start = 0, end = 100, step = 1, numPartitions = 5),
      where = "id < 20",
      expectedNumAffectedRows = 20L,
      expectedOperationMetrics = Map(
        "numCopiedRows" -> 0,
        "numDeletedRows" -> numRemovedRows,
        "numAddedFiles" -> 0,
        "numRemovedFiles" -> numRemovedFiles,
        "numAddedChangeFiles" -> {
          if (testConfig.cdfEnabled
          ) {
            1
          } else {
            0
          }
        }
      ),
      testConfig = testConfig
    )

    executeTest
  }

  testDeleteMetrics("delete one row per file") { testConfig =>
    var numRemovedFiles = 5
    val numRemovedRows = 5
    var numCopiedRows = 95
    var numAddedFiles = if (testConfig.partitioned) 5 else 2
    runDeleteAndCheckMetrics(
      table = spark.range(start = 0, end = 100, step = 1, numPartitions = 5),
      where = "id in (5, 25, 45, 65, 85)",
      expectedNumAffectedRows = 5L,
      expectedOperationMetrics = Map(
        "numCopiedRows" -> numCopiedRows,
        "numDeletedRows" -> numRemovedRows,
        "numAddedFiles" -> numAddedFiles,
        "numRemovedFiles" -> numRemovedFiles,
        "numAddedChangeFiles" -> { if (testConfig.cdfEnabled) numAddedFiles else 0 }
      ),
    testConfig = testConfig
    )
  }

}
