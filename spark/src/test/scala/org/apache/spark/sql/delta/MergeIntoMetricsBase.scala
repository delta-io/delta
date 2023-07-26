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
import org.apache.spark.sql.functions._
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
  import testImplicits._

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

  val testsToIgnore = Seq(
    // The below tests fail due to incorrect numTargetRowsCopied metric.
    "delete-only with condition",
    "delete-only with update with unsatisfied condition",
    "delete-only with unsatisfied condition",
    "delete-only with target-only condition",
    "delete-only with source-only condition",
    "match-only with unsatisfied condition"
  )

  // Helper to generate tests with different configurations.
  private def testMergeMetrics(name: String)(testFn: MergeTestConfiguration => Unit): Unit = {
    for {
      partitioned <- Seq(true, false)
      cdfEnabled <- Seq(true, false)
    } {
      val testConfig = MergeTestConfiguration(partitioned = partitioned, cdfEnabled = cdfEnabled)
      val testName = s"merge-metrics: $name - Partitioned = $partitioned, CDF = $cdfEnabled"

      if (testsToIgnore.contains(name)) {
        // Currently multiple metrics are wrong for Merge. We have added tests for these scenarios
        // but we need to ignore the failing tests until the metrics are fixed.
        ignore(testName) { testFn(testConfig) }
      } else {
        test(testName) { testFn(testConfig) }
      }
    }
  }

  /**
   * Check invariants for row metrics of MERGE INTO command.
   *
   * @param metrics The merge operation metrics from the Delta history.
   */
  private def checkMergeOperationRowMetricsInvariants(metrics: Map[String, String]): Unit = {
    assert(
      metrics("numTargetRowsUpdated").toLong ===
        metrics("numTargetRowsMatchedUpdated").toLong +
        metrics("numTargetRowsNotMatchedBySourceUpdated").toLong)
    assert(
      metrics("numTargetRowsDeleted").toLong ===
        metrics("numTargetRowsMatchedDeleted").toLong +
        metrics("numTargetRowsNotMatchedBySourceDeleted").toLong)
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
      val numBytesAdded = metrics("numTargetBytesAdded").toLong
      val numRowsWritten =
        metrics("numTargetRowsInserted").toLong +
          metrics("numTargetRowsUpdated").toLong +
          metrics("numTargetRowsCopied").toLong
      lazy val assertMsgNumFiles = {
        val expectedNumFilesAdded =
          if (numRowsWritten == 0) "0" else s"between 1 and $numRowsWritten"
        s"""Unexpected value for numTargetFilesAdded metric.
           | Expected: $expectedNumFilesAdded
           | Actual: $numFilesAdded
           | numRowsWritten: $numRowsWritten
           | Metrics: ${metrics.toString}
           |""".stripMargin

      }
      lazy val assertMsgBytes = {
        val expected = if (numRowsWritten == 0) "0" else "greater than 0"
        s"""Unexpected value for numTargetBytesAdded metric.
           | Expected: $expected
           | Actual: $numBytesAdded
           | numRowsWritten: $numRowsWritten
           | numFilesAdded: $numFilesAdded
           | Metrics: ${metrics.toString}
           |""".stripMargin
      }
      if (numRowsWritten == 0) {
        assert(numFilesAdded === 0, assertMsgNumFiles)
        assert(numBytesAdded === 0, assertMsgBytes)
      } else {
        assert(numFilesAdded > 0 && numFilesAdded <= numRowsWritten, assertMsgNumFiles)
        assert(numBytesAdded > 0, assertMsgBytes)
      }
    }

    // numTargetFilesRemoved should have a positive value if rows were updated or deleted and be
    // zero otherwise.  In case of classic merge we also count copied rows as changed, because if
    // match clauses have conditions we may end up copying rows even if no other rows are
    // updated/deleted.
    {
      val numFilesRemoved = metrics("numTargetFilesRemoved").toLong
      val numBytesRemoved = metrics("numTargetBytesRemoved").toLong
      val numRowsTouched =
        metrics("numTargetRowsDeleted").toLong +
          metrics("numTargetRowsUpdated").toLong +
          metrics("numTargetRowsCopied").toLong
      lazy val assertMsgNumFiles = {
        val expectedNumFilesRemoved =
          if (numRowsTouched == 0) "0" else s"between 1 and $numRowsTouched"
        s"""Unexpected value for numTargetFilesRemoved metric.
           | Expected: $expectedNumFilesRemoved
           | Actual: $numFilesRemoved
           | numRowsTouched: $numRowsTouched
           | Metrics: ${metrics.toString}
           |""".stripMargin
      }
      lazy val assertMsgBytes = {
        val expectedNumBytesRemoved =
          if (numRowsTouched == 0) "0" else "greater than 0"
        s"""Unexpected value for numTargetBytesRemoved metric.
           | Expected: $expectedNumBytesRemoved
           | Actual: $numBytesRemoved
           | numRowsTouched: $numRowsTouched
           | Metrics: ${metrics.toString}
           |""".stripMargin
      }

      if (numRowsTouched == 0) {
        assert(numFilesRemoved === 0, assertMsgNumFiles)
        assert(numBytesRemoved === 0, assertMsgBytes)
      } else {
        assert(numFilesRemoved > 0 && numFilesRemoved <= numRowsTouched, assertMsgNumFiles)
        assert(numBytesRemoved > 0, assertMsgBytes)
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
   * @param overrideExpectedOpMetrics Sequence of expected operation metric values to override from
   *                                  those provided in expectedOpMetrics for specific
   *                                  configurations of partitioned and cdfEnabled. Elements
   *                                  provided as:
   *                                  ((partitioned, cdfEnabled), (metric_name, metric_value))
   */
  private def runMergeCmdAndTestMetrics(
      targetDf: DataFrame,
      sourceDf: DataFrame,
      mergeCmdFn: MergeCmd,
      expectedOpMetrics: Map[String, Int],
      testConfig: MergeTestConfiguration,
      overrideExpectedOpMetrics: Seq[((Boolean, Boolean), (String, Int))] = Seq.empty
  ): Unit = {
    withSQLConf(
      DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_SKIP_RECORDING_EMPTY_COMMITS.key -> "false",
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
        var expectedOpMetricsWithDefaults = metricsWithDefaultZeroValue ++
          expectedOpMetrics.filter(m => m._2 >= 0).mapValues(_.toString)

        overrideExpectedOpMetrics.foreach { case ((partitioned, cdfEnabled), (metric, value)) =>
          if (partitioned == testConfig.partitioned && cdfEnabled == testConfig.cdfEnabled) {
            expectedOpMetricsWithDefaults = expectedOpMetricsWithDefaults +
              (metric -> value.toString)
          }
        }

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
        // Check row metrics invariants.
        checkMergeOperationRowMetricsInvariants(operationMetrics)
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

  testMergeMetrics("insert-only with skipping") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 100, end = 200, step = 1, numPartitions = 5).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable.as("t")
        .merge(sourceDf.as("s"), "s.id = t.id and t.partCol >= 2")
        .whenNotMatched()
        .insertAll()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100,
      "numOutputRows" -> 100,
      "numTargetRowsInserted" -> 100
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("insert-only with condition") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenNotMatched("s.id >= 125")
        .insertAll()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100,
      "numOutputRows" -> 25,
      "numTargetRowsInserted" -> 25
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("insert-only when all rows match") { testConfig => {
    val targetDf = spark.range(start = 0, end = 200, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenNotMatched()
        .insertAll()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("insert-only with unsatisfied condition") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenNotMatched("s.id > 150")
        .insertAll()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("insert-only with empty source") { testConfig => {
    val targetDf = spark.range(start = 0, end = 200, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(0).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenNotMatched()
        .insertAll()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 0
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("insert-only with empty target") { testConfig => {
    val targetDf = spark.range(0).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenNotMatched()
        .insertAll()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100,
      "numOutputRows" -> 100,
      "numTargetRowsInserted" -> 100
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("insert-only with disjoint tables") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 100, end = 200, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100,
      "numOutputRows" -> 100,
      "numTargetRowsInserted" -> 100
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("insert-only with update/delete with unsatisfied conditions") { testConfig => {
    val targetDf = spark.range(start = 0, end = 50, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 0, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched("s.id + t.id > 200")
        .updateAll()
        .whenMatched("s.id + t.id < 0")
        .delete()
        .whenNotMatched()
        .insertAll()
        .execute()
    }
    // In classic merge we are copying all rows from job1.
    val expectedOpMetrics = Map(
      "numSourceRows" -> 150,
      "numOutputRows" -> 150,
      "numTargetRowsInserted" -> 100,
      "numTargetRowsCopied" -> 50,
      "numTargetFilesRemoved" -> 5
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  /////////////////////////////
  // delete-only merge tests //
  /////////////////////////////

  testMergeMetrics("delete-only") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched()
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 100,
      "numTargetRowsDeleted" -> 50,
      "numTargetRowsMatchedDeleted" -> 50,
      "numTargetRowsRemoved" -> -1,
      "numOutputRows" -> 10,
      "numTargetRowsCopied" -> 10,
      "numTargetFilesAdded" -> 1,
      "numTargetFilesRemoved" -> -1
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only with skipping") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id and t.partCol >= 2")
        .whenMatched()
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 100,
      "numOutputRows" -> 10,
      "numTargetRowsCopied" -> 10,
      "numTargetRowsDeleted" -> 50,
      "numTargetRowsMatchedDeleted" -> 50,
      "numTargetRowsRemoved" -> -1,
      "numTargetFilesAdded" -> 1,
      "numTargetFilesRemoved" -> 3
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only with disjoint tables") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 100, end = 200, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched()
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100,
      "numTargetFilesAdded" -> 0,
      "numTargetFilesRemoved" -> 0
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only delete all rows") { testConfig => {
    val targetDf = spark.range(start = 100, end = 200, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 0, end = 300, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched()
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 300,
      "numOutputRows" -> 0,
      "numTargetRowsCopied" -> 0,
      "numTargetRowsDeleted" -> 100,
      "numTargetRowsMatchedDeleted" -> 100,
      "numTargetRowsRemoved" -> -1,
      "numTargetFilesAdded" -> 0,
      "numTargetFilesRemoved" -> 5
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only with condition") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 0, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched("s.id + t.id < 50")
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 150,
      "numOutputRows" -> 15,
      "numTargetRowsCopied" -> 15,
      "numTargetRowsDeleted" -> 25,
      "numTargetRowsMatchedDeleted" -> 25,
      "numTargetRowsRemoved" -> -1,
      "numTargetFilesAdded" -> 1,
      "numTargetFilesRemoved" -> 2
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only with update with unsatisfied condition") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched("s.id + t.id > 1000")
        .updateAll()
        .whenMatched("s.id + t.id < 50")
        .delete()
        .execute()
    }
    // In case of partitioned tables, files are mixed-in even though finally there are no matches.
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 100,
      "numOutputRows" -> 15,
      "numTargetRowsCopied" -> 15,
      "numTargetRowsDeleted" -> 25,
      "numTargetRowsMatchedDeleted" -> 25,
      "numTargetRowsRemoved" -> -1,
      "numTargetFilesAdded" -> 1,
      "numTargetFilesRemoved" -> 2
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only with condition on delete and insert with no matching rows") {
    testConfig => {
      val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
      val sourceDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 10).toDF()
      val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
        targetTable
          .as("t")
          .merge(sourceDf.as("s"), "s.id = t.id")
          .whenMatched("s.id + t.id < 50")
          .delete()
          .whenNotMatched()
          .insertAll()
          .execute()
      }
      // In classic merge we are copying all rows from job1.
      // In case of partitioned tables, files are mixed-in even though finally there are no matches.
      val expectedOpMetrics = Map[String, Int](
        "numSourceRows" -> 100,
        "numOutputRows" -> 75,
        "numTargetRowsCopied" -> 75,
        "numTargetRowsDeleted" -> 25,
        "numTargetRowsMatchedDeleted" -> 25,
        "numTargetRowsRemoved" -> -1,
        "numTargetFilesRemoved" -> 5
      )
      runMergeCmdAndTestMetrics(
        targetDf = targetDf,
        sourceDf = sourceDf,
        mergeCmdFn = mergeCmdFn,
        expectedOpMetrics = expectedOpMetrics,
        testConfig = testConfig
      )
    }
  }

  testMergeMetrics("delete-only with unsatisfied condition") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 0, end = 150, step = 1, numPartitions = 15).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched("s.id + t.id > 1000")
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 150,
      "numTargetFilesAdded" -> 0,
      "numTargetFilesRemoved" -> 0
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only with target-only condition") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 0, end = 150, step = 1, numPartitions = 15).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched("t.id >= 45")
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 150,
      "numOutputRows" -> 5,
      "numTargetRowsCopied" -> 5,
      "numTargetRowsDeleted" -> 55,
      "numTargetRowsMatchedDeleted" -> 55,
      "numTargetRowsRemoved" -> -1,
      "numTargetFilesAdded" -> 1,
      "numTargetFilesRemoved" -> 3
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only with source-only condition") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 100).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched("s.id >= 70")
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 100,
      "numOutputRows" -> 10,
      "numTargetRowsCopied" -> 10,
      "numTargetRowsDeleted" -> 30,
      "numTargetRowsMatchedDeleted" -> 30,
      "numTargetRowsRemoved" -> -1,
      "numTargetFilesAdded" -> 1,
      "numTargetFilesRemoved" -> 2
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only with empty source") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 4).toDF()
    val sourceDf = spark.range(0).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched("t.id > 25")
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 0,
      "numTargetFilesAdded" -> 0,
      "numTargetFilesRemoved" -> 0
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only with empty target") { testConfig => {
    val targetDf = spark.range(0).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 3).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched()
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map(
      // This actually goes through a special code path in MERGE because the optimizer optimizes
      // away the join to the source table entirely if the target table is empty.
      "numSourceRows" -> 100,
      "numTargetFilesAdded" -> 0,
      "numTargetFilesRemoved" -> 0
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only without join empty source") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(0).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "t.id >= 50")
        .whenMatched()
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 0,
      "numTargetFilesAdded" -> 0,
      "numTargetFilesRemoved" -> 0
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only without join with source with 1 row") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 0, end = 1, step = 1, numPartitions = 1).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "t.id >= 50")
        .whenMatched()
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 1,
      "numOutputRows" -> 10,
      "numTargetRowsCopied" -> 10,
      "numTargetRowsDeleted" -> 50,
      "numTargetRowsMatchedDeleted" -> 50,
      "numTargetRowsRemoved" -> -1,
      "numTargetFilesAdded" -> 1,
      "numTargetFilesRemoved" -> 3
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only without join") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 0, end = 200, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "t.id >= 50")
        .whenMatched()
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 200,
      "numOutputRows" -> 10,
      "numTargetRowsCopied" -> 10,
      "numTargetRowsDeleted" -> 50,
      "numTargetRowsMatchedDeleted" -> 50,
      "numTargetRowsRemoved" -> -1,
      "numTargetFilesAdded" -> 1,
      "numTargetFilesRemoved" -> 3
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("delete-only with duplicates") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    // This will cause duplicates due to rounding.
    val sourceDf = spark
      .range(start = 50, end = 150, step = 1, numPartitions = 2)
      .toDF()
      .select(floor($"id" / 2).as("id"))
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched()
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 100,
      "numOutputRows" -> 10,
      "numTargetRowsDeleted" -> 50,
      "numTargetRowsMatchedDeleted" -> 50,
      "numTargetRowsRemoved" -> -1,
      "numTargetRowsCopied" -> 10,
      "numTargetFilesAdded" -> 2,
      "numTargetFilesRemoved" -> 3
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig,
      // When cdf=true in this test we hit the corner case where there are duplicate matches with a
      // delete clause and we generate duplicate cdc data. This is further detailed in
      // MergeIntoCommand at the definition of isDeleteWithDuplicateMatchesAndCdc. Our fix for this
      // scenario includes deduplicating the output data which reshuffles the output data.
      // Thus when the table is not partitioned, the data is rewritten into 1 new file rather than 2
      overrideExpectedOpMetrics = Seq(((false, true), ("numTargetFilesAdded", 1)))
    )
  }}

  /////////////////////////////
  // match-only merge tests  //
  /////////////////////////////
  testMergeMetrics("match-only") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched()
        .updateAll()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100,
      "numOutputRows" -> 60,
      "numTargetRowsUpdated" -> 50,
      "numTargetRowsMatchedUpdated" -> 50,
      "numTargetRowsCopied" -> 10,
      "numTargetFilesRemoved" -> 3
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("match-only with skipping") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id and t.partCol >= 2")
        .whenMatched()
        .updateAll()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100,
      "numOutputRows" -> 60,
      "numTargetRowsUpdated" -> 50,
      "numTargetRowsMatchedUpdated" -> 50,
      "numTargetRowsCopied" -> 10,
      "numTargetFilesRemoved" -> 3
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("match-only with update/delete with unsatisfied conditions") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched("s.id + t.id > 1000")
        .delete()
        .whenMatched("s.id + t.id < 1000")
        .updateAll()
        .whenNotMatched("s.id > 1000")
        .insertAll()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100,
      "numOutputRows" -> 60,
      "numTargetRowsUpdated" -> 50,
      "numTargetRowsMatchedUpdated" -> 50,
      "numTargetRowsCopied" -> 10,
      "numTargetFilesRemoved" -> 3
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("match-only with unsatisfied condition") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched("s.id + t.id > 1000")
        .updateAll()
        .execute()
    }

    val expectedOpMetrics = Map(
      "numSourceRows" -> 100
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  /////////////////////////////////////////////
  // not matched by source only merge tests  //
  /////////////////////////////////////////////
  testMergeMetrics("not matched by source update only") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenNotMatchedBySource("t.id < 20")
        .updateExpr(Map("t.extraCol" -> "t.extraCol + 1"))
        .execute()
    }
    val expectedOpMetrics = Map[String, Int](
      "numSourceRows" -> 100,
      "numOutputRows" -> 100,
      "numTargetRowsUpdated" -> 20,
      "numTargetRowsNotMatchedBySourceUpdated" -> 20,
      "numTargetRowsCopied" -> 80,
      "numTargetFilesRemoved" -> 5
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
   }}

  /////////////////////////////
  //    full merge tests     //
  /////////////////////////////
  testMergeMetrics("upsert") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100,
      "numOutputRows" -> 110,
      "numTargetRowsInserted" -> 50,
      "numTargetRowsUpdated" -> 50,
      "numTargetRowsMatchedUpdated" -> 50,
      "numTargetRowsCopied" -> 10,
      "numTargetFilesRemoved" -> 3
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("replace target with source") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .whenNotMatchedBySource()
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100,
      "numOutputRows" -> 100,
      "numTargetRowsInserted" -> 50,
      "numTargetRowsUpdated" -> 50,
      "numTargetRowsMatchedUpdated" -> 50,
      "numTargetRowsDeleted" -> 50,
      "numTargetRowsNotMatchedBySourceDeleted" -> 50,
      "numTargetRowsCopied" -> 0,
      "numTargetFilesRemoved" -> 5
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics("upsert and delete with conditions") { testConfig => {
    val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 10).toDF()
    val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 3).toDF()
    val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
        .as("t")
        .merge(sourceDf.as("s"), "s.id = t.id")
        .whenMatched("t.id >= 55 and t.id < 60")
        .updateAll()
        .whenMatched("t.id < 70")
        .delete()
        .whenNotMatched()
        .insertAll()
        .whenNotMatchedBySource("t.id < 10")
        .updateExpr(Map("t.extraCol" -> "t.extraCol + 1"))
        .whenNotMatchedBySource("t.id >= 45")
        .delete()
        .execute()
    }
    val expectedOpMetrics = Map(
      "numSourceRows" -> 100,
      "numOutputRows" -> 130,
      "numTargetRowsInserted" -> 50,
      "numTargetRowsUpdated" -> 15,
      "numTargetRowsMatchedUpdated" -> 5,
      "numTargetRowsNotMatchedBySourceUpdated" -> 10,
      "numTargetRowsDeleted" -> 20,
      "numTargetRowsMatchedDeleted" -> 15,
      "numTargetRowsNotMatchedBySourceDeleted" -> 5,
      "numTargetRowsCopied" -> 65,
      "numTargetFilesRemoved" -> 10
    )
    runMergeCmdAndTestMetrics(
      targetDf = targetDf,
      sourceDf = sourceDf,
      mergeCmdFn = mergeCmdFn,
      expectedOpMetrics = expectedOpMetrics,
      testConfig = testConfig
    )
  }}

  testMergeMetrics(
    "update/delete/insert with some unsatisfied conditions") {
    testConfig => {
      val targetDf = spark.range(start = 0, end = 100, step = 1, numPartitions = 5).toDF()
      val sourceDf = spark.range(start = 50, end = 150, step = 1, numPartitions = 10).toDF()
      val mergeCmdFn: MergeCmd = (targetTable, sourceDf) => {
      targetTable
          .as("t")
          .merge(sourceDf.as("s"), "s.id = t.id")
          .whenMatched("s.id + t.id > 1000")
          .delete()
          .whenNotMatchedBySource("t.id > 1000")
          .delete()
          .whenNotMatchedBySource("t.id < 1000")
          .updateExpr(Map("t.extraCol" -> "t.extraCol + 1"))
          .whenNotMatched("s.id > 1000")
          .insertAll()
          .execute()
      }
      val expectedOpMetrics = Map[String, Int](
        "numSourceRows" -> 100,
        "numOutputRows" -> 100,
        "numTargetRowsUpdated" -> 50,
        "numTargetRowsNotMatchedBySourceUpdated" -> 50,
        "numTargetRowsCopied" -> 50,
        "numTargetFilesRemoved" -> 5
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
    "numTargetRowsMatchedUpdated",
    "numTargetRowsNotMatchedBySourceUpdated",
    "numTargetRowsDeleted",
    "numTargetRowsMatchedDeleted",
    "numTargetRowsNotMatchedBySourceDeleted",
    "numTargetRowsCopied",
    "numOutputRows"
  )
  // Metrics related with affected number of files. Values depend on the file layout.
  val mergeFileMetrics = Set(
    "numTargetFilesAdded", "numTargetFilesRemoved", "numTargetBytesAdded", "numTargetBytesRemoved")
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
