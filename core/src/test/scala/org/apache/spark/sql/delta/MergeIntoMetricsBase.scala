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

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for the metrics of MERGE INTO command in Delta log.
 *
 * This test suite checks the values of metrics that are emitted in Delta log by MERGE INTO command,
 * with Changed Data Feed (CDF) enabled/disabled.
 *
 * Metrics related with number of affected rows are deterministic and so the expected values are
 * explicitly checked. Metrics related with number of affected files and execution times are not
 * deterministic, and so we check only their presence and some invariants.
 *
 */
trait MergeIntoMetricsBase
  extends QueryTest
  with SharedSparkSession { self: DescribeDeltaHistorySuiteBase =>

  import MergeIntoMetricsBase._

  ///////////////////////
  // container classes //
  ///////////////////////

  private case class MergeTestConfiguration(partitioned: Boolean, cdfEnabled: Boolean) {

    /** Return a [[MetricValue]] for this config with the provided default value. */
    def metricValue(defaultValue: Int): MetricValue = {
      new MetricValue(this, defaultValue)
    }
  }

  /**
   * Helper class to compute values of metrics that depend on the configuration.
   *
   * Objects are initialized with a test configuration and a default value. The value can then be
   * overwritten with helper methods that check the test config and value() can be called to
   * retrieve the final expected value for a test.
   */
  private class MetricValue(testConfig: MergeTestConfiguration, defaultValue: Int) {
    private var currentValue: Int = defaultValue

    def value: Int = currentValue

    // e.g. ifCDF
  }

  ////////////////
  // test utils //
  ////////////////

  // Helper to generate tests with different configurations.
  private def testMergeMetrics(name: String)(testFn: MergeTestConfiguration => Unit): Unit = {
    for {
      partitioned <- Seq(true, false)
      cdfEnabled <- Seq(true, false)
    } {
      val testConfig = MergeTestConfiguration(partitioned = partitioned, cdfEnabled = cdfEnabled)
      val testName = s"merge-metrics: $name - Partitioned = $partitioned, CDF = $cdfEnabled"
      test(testName) { testFn(testConfig) }
    }
  }

  /**
   * Check invariants for file metrics of MERGE INTO command.
   *
   * @param metrics The merge operation metrics from the Delta history.
   */
  private def checkMergeOperationFileMetricsInvariants(metrics: Map[String, String]): Unit = {
    // numTargetFilesAdded should have a positive value if rows were added and be zero
    // otherwise.
    {
      val numFilesAdded = metrics("numTargetFilesAdded").toLong
      val numRowsWritten =
        metrics("numTargetRowsInserted").toLong +
          metrics("numTargetRowsUpdated").toLong +
          metrics("numTargetRowsCopied").toLong
      lazy val assertMsg = {
        val expectedNumFilesAdded =
          if (numRowsWritten == 0) "0" else s"between 1 and $numRowsWritten"
        s"""Unexpected value for numTargetFilesAdded metric.
           | Expected: $expectedNumFilesAdded
           | Actual: $numFilesAdded
           | numRowsWritten: $numRowsWritten
           | Metrics: ${metrics.toString}
           |""".stripMargin
      }
      if (numRowsWritten == 0) {
        assert(numFilesAdded === 0, assertMsg)
      } else {
        assert(numFilesAdded > 0 && numFilesAdded <= numRowsWritten, assertMsg)
      }
    }

    // numTargetFilesRemoved should have a positive value if rows were updated or deleted and be
    // zero otherwise.  In case of classic merge we also count copied rows as changed, because if
    // match clauses have conditions we may end up copying rows even if no other rows are
    // updated/deleted.
    {
      val numFilesRemoved = metrics("numTargetFilesRemoved").toLong
      val numRowsTouched =
        metrics("numTargetRowsDeleted").toLong +
          metrics("numTargetRowsUpdated").toLong +
          metrics("numTargetRowsCopied").toLong
      lazy val assertMsg = {
        val expectedNumFilesRemoved =
          if (numRowsTouched == 0) "0" else s"between 1 and $numRowsTouched"
        s"""Unexpected value for numTargetFilesRemoved metric.
           | Expected: $expectedNumFilesRemoved
           | Actual: $numFilesRemoved
           | numRowsTouched: $numRowsTouched
           | Metrics: ${metrics.toString}
           |""".stripMargin
      }
      if (numRowsTouched == 0) {
        assert(numFilesRemoved === 0, assertMsg)
      } else {
        assert(numFilesRemoved > 0 && numFilesRemoved <= numRowsTouched, assertMsg)
      }
    }
  }

  /**
   * Helper method to create a target table with the desired options, run a merge command and check
   * the operation metrics in the Delta history.
   *
   * For operation metrics the following checks are performed:
   * a) The operation metrics in Delta history must match [[DeltaOperationMetrics.MERGE]] schema,
   *    i.e. no metrics can be missing or unknown metrics can exist.
   * b) All operation metrics must have a non-negative values.
   * c) The values of metrics that are specified in 'expectedOpMetrics' argument must match the
   * operation metrics. Metrics with a value of -1 are ignored, to allow callers always specify
   * metrics that don't exist under some configurations.
   * d) Row-related operation metrics that are not specified in 'expectedOpMetrics' must be zero.
   * e) File/Time-related operation metrics that are not specified in 'expectedOpMetrics' can have
   * non-zero values. These metrics are not deterministic and so this method only checks that
   * some invariants hold.
   *
   * @param targetDf The DataFrame to generate the target table for the merge command.
   * @param sourceDf The DataFrame to generate the source table for the merge command.
   * @param mergeCmdFn The function that actually runs the merge command.
   * @param expectedOpMetrics A map with values for expected operation metrics.
   * @param testConfig The configuration options for this test
   */
  private def runMergeCmdAndTestMetrics(
      targetDf: DataFrame,
      sourceDf: DataFrame,
      mergeCmdFn: MergeCmd,
      expectedOpMetrics: Map[String, Int],
      testConfig: MergeTestConfiguration): Unit = {
    withSQLConf(
      DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true",
      DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> testConfig.cdfEnabled.toString
    ) {
      withTempDir { tempDir =>
        def addExtraColumns(tableDf: DataFrame): DataFrame = {
          // Add a column to be used for data partitioning and one extra column for filters in
          // queries.
          val numRows = tableDf.count()
          val numPartitions = tableDf.rdd.getNumPartitions
          val numRowsPerPart = if (numRows > 0) numRows / numPartitions else 1
          tableDf.withColumn("partCol", expr(s"floor(id / $numRowsPerPart)"))
            .withColumn("extraCol", expr(s"$numRows - id"))
        }

        // Add extra columns and create target table.
        val tempPath = tempDir.getAbsolutePath
        val partitionBy = if (testConfig.partitioned) Seq("partCol") else Seq()
        val targetDfWithExtraCols = addExtraColumns(targetDf)
        targetDfWithExtraCols
          .write
          .partitionBy(partitionBy: _*)
          .format("delta")
          .save(tempPath)
        val targetTable = io.delta.tables.DeltaTable.forPath(tempPath)

        // Also add extra columns in source to be able to call updateAll()/insertAll().
        val sourceDfWithExtraCols = addExtraColumns(sourceDf)

        // Run MERGE INTO command
        mergeCmdFn(targetTable, sourceDfWithExtraCols)

        // Query the operation metrics from the Delta log history.
        val operationMetrics: Map[String, String] = getOperationMetrics(targetTable.history(1))

        // Get the default row operation metrics and override them with the provided ones.
        val metricsWithDefaultZeroValue = mergeRowMetrics.map(_ -> "0").toMap
        val expectedOpMetricsWithDefaults = metricsWithDefaultZeroValue ++
          expectedOpMetrics.filter(m => m._2 >= 0).mapValues(_.toString)

        // Check that all operation metrics are positive numbers.
        for ((metricName, metricValue) <- operationMetrics) {
          assert(metricValue.toLong >= 0,
            s"Invalid negative value for metric $metricName = $metricValue")
        }

        // Check that operation metrics match the schema and that values match the expected ones.
        checkOperationMetrics(
          expectedOpMetricsWithDefaults,
          operationMetrics,
          DeltaOperationMetrics.MERGE
        )
        // Check file metrics invariants.
        checkMergeOperationFileMetricsInvariants(operationMetrics)
        // Check time metrics invariants.
        checkOperationTimeMetricsInvariant(mergeTimeMetrics, operationMetrics)
        // Check CDF metrics invariants.
        checkMergeOperationCdfMetricsInvariants(operationMetrics, testConfig.cdfEnabled)
      }
    }
  }

  /////////////////////////////
  // insert-only merge tests //
  /////////////////////////////

  testMergeMetrics("insert-only") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable.as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenNotMatched()
        .insertAll()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100,
      "numOutputRows" -> 50,
      "numTargetRowsInserted" -> 50
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}
}

object MergeIntoMetricsBase extends QueryTest with SharedSparkSession {

  ///////////////////////
  // helpful constants //
  ///////////////////////

  // Metrics related with affected number of rows. Values should always be deterministic.
  val mergeRowMetrics = Set(
    "numSourceRows",
    "numTargetRowsInserted",
    "numTargetRowsUpdated",
    "numTargetRowsDeleted",
    "numTargetRowsCopied",
    "numOutputRows"
  )
  // Metrics related with affected number of files. Values depend on the file layout.
  val mergeFileMetrics = Set("numTargetFilesAdded", "numTargetFilesRemoved")
  // Metrics related with execution times.
  val mergeTimeMetrics = Set("executionTimeMs", "scanTimeMs", "rewriteTimeMs")
  // Metrics related with CDF. Available only when CDF is available.
  val mergeCdfMetrics = Set("numTargetChangeFilesAdded")

  // Ensure that all metrics are properly copied here.
  assert(
    DeltaOperationMetrics.MERGE.size ==
      mergeRowMetrics.size + mergeFileMetrics.size + mergeTimeMetrics.size + mergeCdfMetrics.size
  )

  ///////////////////
  // helpful types //
  ///////////////////

  type MergeCmd = (io.delta.tables.DeltaTable, DataFrame) => Unit

  /////////////////////
  // helpful methods //
  /////////////////////

  /**
   * Check invariants for the CDF metrics of MERGE INTO command. Checking the actual values
   * is avoided since they depend on the file layout and the type of merge.
   *
   * @param metrics The merge operation metrics from the Delta history.
   * @param cdfEnabled Whether CDF was enabled or not.
   */
  def checkMergeOperationCdfMetricsInvariants(
      metrics: Map[String, String],
      cdfEnabled: Boolean): Unit = {
    val numRowsUpdated = metrics("numTargetRowsUpdated").toLong
    val numRowsDeleted = metrics("numTargetRowsDeleted").toLong
    val numRowsInserted = metrics("numTargetRowsInserted").toLong
    val numRowsChanged = numRowsUpdated + numRowsDeleted + numRowsInserted
    val numTargetChangeFilesAdded = metrics("numTargetChangeFilesAdded").toLong

    lazy val assertMsg =
      s"""Unexpected value for numTargetChangeFilesAdded metric:
         | Expected : ${if (numRowsChanged == 0) 0 else "Positive integer value"}
         | Actual : $numTargetChangeFilesAdded
         | cdfEnabled: $cdfEnabled
         | numRowsChanged: $numRowsChanged
         | Metrics: ${metrics.toString}
         |""".stripMargin

    if (!cdfEnabled || numRowsChanged == 0) {
      assert(numTargetChangeFilesAdded === 0, assertMsg)
    } else {
      // In case of insert-only merges where only new files are added, CDF data are not required
      // since the CDF reader can read the corresponding added files. However, there are cases
      // where we produce CDF data even in insert-only merges (see 'insert-only-dynamic-predicate'
      // testcase for an example). Here we skip the assertion, since both behaviours can be
      // considered valid.
      val isInsertOnly = numRowsInserted > 0 && numRowsChanged == numRowsInserted
      if (!isInsertOnly) {
        assert(numTargetChangeFilesAdded > 0, assertMsg)
      }
    }
  }
}
