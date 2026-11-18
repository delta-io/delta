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

package org.apache.spark.sql.delta.uniform

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaTestUtils.filterUsageRecords
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Base classes for all UniForm end-to-end test cases. Provides support to
 * write data with Delta SparkSession and read data for verification.
 *
 * People who need to write a new test suite should extend this class and
 * implement their test cases with [[write]] and [[readAndVerify]], which execute
 * with the writer and reader respectively.
 *
 * Implementing classes need to correctly set up the reader and writer environments.
 * See [[UniFormE2EIcebergSuiteBase]] for existing examples.
 */

case class VerifyFullOrIncremental(tableName: String, isIncremental: Boolean)

trait UniFormE2ETest
  extends QueryTest
  with SharedSparkSession {

  /**
   * Execute write operations through the writer SparkSession
   *
   * @param sqlText write query to the UniForm table
   */
  protected def write(sqlText: String): DataFrame = sql(sqlText)

  protected def writeAndVerify(
      sqlText: String,
      isAtomicMode: Boolean,
      verifyFullOrIncrementalOpt: Option[VerifyFullOrIncremental] = None): Unit = {
    val events = Log4jUsageLogger.track { sql(sqlText) }
    verifyConversionMode(events, isAtomicMode)
    verifyFullOrIncrementalOpt.foreach {
      case VerifyFullOrIncremental(tbl, true) =>
        verifyIncrementalConversion(tbl, events)
      case VerifyFullOrIncremental(tbl, false) =>
        verifyFullConversion(tbl, events)
    }
  }

  /**
   * Verify the result by reading from the reader session and compare the result to the expected.
   *
   * @param table  write table name
   * @param fields fields to verify, separated by comma. E.g., "col1, col2"
   * @param orderBy fields to order the results, separated by comma.
   * @param expect expected result
   */
  protected def readAndVerify(
      table: String, fields: String, orderBy: String, expect: Seq[Row]): Unit =
    throw new UnsupportedOperationException

  /**
   * Subclasses should override this method when the table name for reading
   * is different from the table name used for writing. For example, when we
   * write a table using the name `table1`, and then read it from another catalog
   * `catalog_read`, this method should return `catalog_read.default.table1`
   * for the input `table1`.
   *
   * @param tableName table name for writing (name only)
   * @return table name for reading, default is no translation
   */
  protected def tableNameForRead(tableName: String): String = tableName

  protected def verifyIncrementalConversion(
      tableName: String,
      events: Seq[UsageRecord]): Unit = {
    val toVersion = DeltaLog.forTable(spark, TableIdentifier(tableName)).update().version
    val fromVersion = toVersion
    val rangeEvents = filterUsageRecords(events, "delta.iceberg.conversion.deltaCommitRange")
    assert(rangeEvents.nonEmpty, "Expected deltaCommitRange event proving incremental conversion")
    val eventData = JsonUtils.fromJson[Map[String, Any]](rangeEvents.head.blob)
    assert(eventData("fromVersion") === fromVersion, s"Expected fromVersion=$fromVersion")
    assert(eventData("toVersion") === toVersion, s"Expected toVersion=$toVersion")
  }

  protected def verifyFullConversion(tableName: String, events: Seq[UsageRecord]): Unit = {
    val snapshot = DeltaLog.forTable(spark, TableIdentifier(tableName)).update()
    val batchEvents = filterUsageRecords(events, "delta.iceberg.conversion.batch")
    assert(batchEvents.nonEmpty, "Expected batch event proving full conversion")
    val eventData = JsonUtils.fromJson[Map[String, Any]](batchEvents.head.blob)
    assert(eventData("version") === snapshot.version, s"Expected version=${snapshot.version}")
  }

  // Verify if post-commit-conversion or atomic-UniForm conversion is triggered
  private def verifyConversionMode(events: Seq[UsageRecord], isAtomicMode: Boolean): Unit = {
    val postCommitConversionExists =
      filterUsageRecords(events, "delta.iceberg.conversion.convertSnapshot").nonEmpty
    val preCommitConversionExists =
      filterUsageRecords(events, "delta.iceberg.conversion.convertUncommitedTxn").nonEmpty
    if (isAtomicMode) {
      assert(preCommitConversionExists && !postCommitConversionExists)
    } else {
      assert(!preCommitConversionExists && postCommitConversionExists)
    }
  }
}
