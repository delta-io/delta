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

package org.apache.spark.sql.delta.skipping

import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumn, ClusteringColumnInfo}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.DeltaOperations.{CLUSTERING_PARAMETER_KEY, ZORDER_PARAMETER_KEY}
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

trait ClusteredTableTestUtilsBase extends SparkFunSuite with SharedSparkSession {
  import testImplicits._

  /**
   * Helper for running optimize on the table with different APIs.
   * @param table the name of table
   */
  def optimizeTable(table: String): DataFrame = {
    sql(s"OPTIMIZE $table")
  }

  /**
   * Runs optimize on the table and calls postHook on the metrics.
   * @param table the name of table
   * @param postHook callback triggered with OptimizeMetrics returned by the OPTIMIZE command
   */
  def runOptimize(table: String)(postHook: OptimizeMetrics => Unit): Unit = {
    postHook(optimizeTable(table).select($"metrics.*").as[OptimizeMetrics].head())

    // Verify Delta history operation parameters' clusterBy
    verifyDescribeHistoryOperationParameters(table)
  }

  def verifyClusteringColumnsInDomainMetadata(
      snapshot: Snapshot,
      expectedLogicalClusteringColumns: String): Unit = {
    val logicalColumnNames = if (expectedLogicalClusteringColumns.trim.isEmpty) {
      Seq.empty[String]
    } else {
      expectedLogicalClusteringColumns.split(",").map(_.trim).toSeq
    }
    val expectedClusteringColumns = logicalColumnNames.map(ClusteringColumn(snapshot.schema, _))
    val actualClusteringColumns =
      ClusteredTableUtils.getClusteringColumnsOptional(snapshot).getOrElse(Seq.empty)
    assert(expectedClusteringColumns == actualClusteringColumns)
  }

  // Verify the operation parameters of the last history event contains `clusterBy`.
  protected def verifyDescribeHistoryOperationParameters(
      table: String
  ): Unit = {
    val clusterBySupportedOperations = Set(
      "CREATE TABLE",
      "REPLACE TABLE",
      "CREATE OR REPLACE TABLE",
      "CREATE TABLE AS SELECT",
      "REPLACE TABLE AS SELECT",
      "CREATE OR REPLACE TABLE AS SELECT")

    val isPathBasedTable = table.startsWith("tahoe.") || table.startsWith("delta.")
    val (deltaLog, snapshot) = if (isPathBasedTable) {
      // Path based table.
      DeltaLog.forTableWithSnapshot(spark, table.drop(6).replace("`", ""))
    } else {
      DeltaLog.forTableWithSnapshot(spark, TableIdentifier(table))
    }
    val isClusteredTable = ClusteredTableUtils.isSupported(snapshot.protocol)
    val clusteringColumns =
      ClusteringColumnInfo.extractLogicalNames(snapshot)
    val expectedClusterBy = JsonUtils.toJson(clusteringColumns)
    val expectClustering = isClusteredTable && clusteringColumns.nonEmpty

    val lastEvent = deltaLog.history.getHistory(Some(1)).head
    val lastOperationParameters = lastEvent.operationParameters

    def doAssert(assertion: => Boolean): Unit = {
      val debugMsg = "verifyDescribeHistoryOperationParameters DEBUG: " +
        "assert failed. Please check the expected behavior and " +
        "add the operation to the appropriate case in " +
        "verifyDescribeHistoryOperationParameters. " +
        s"table: $table, lastOperation: ${lastEvent.operation} " +
        s"lastOperationParameters: $lastOperationParameters"
      try {
        assert(assertion, debugMsg)
      } catch {
        case e: Throwable =>
          throw new Throwable(debugMsg, e)
      }
    }

    // Check clusterBy exists and matches the expected clusterBy.
    def assertClusterByExists(): Unit = {
      doAssert(lastOperationParameters(CLUSTERING_PARAMETER_KEY) === expectedClusterBy)
    }

    // Check clusterBy does not exist or is empty.
    def assertClusterByEmptyOrNotExists(): Unit = {
      doAssert(!lastOperationParameters.contains(CLUSTERING_PARAMETER_KEY) ||
        lastOperationParameters(CLUSTERING_PARAMETER_KEY) === "[]")
    }

    // Check clusterBy does not exist.
    def assertClusterByNotExist(): Unit = {
      doAssert(!lastOperationParameters.contains(CLUSTERING_PARAMETER_KEY))
    }

    // Check clusterBy
    lastEvent.operation match {
      case "CLUSTER BY" =>
        // Operation is [[DeltaOperations.ClusterBy]] - ALTER TABLE CLUSTER BY
        doAssert(
          lastOperationParameters("newClusteringColumns") === clusteringColumns.mkString(",")
        )
      case "OPTIMIZE" =>
        if (expectClustering) {
          doAssert(lastOperationParameters(CLUSTERING_PARAMETER_KEY) === expectedClusterBy)
          doAssert(lastOperationParameters(ZORDER_PARAMETER_KEY) === "[]")
        } else {
          // If the table clusters by NONE, OPTIMIZE will be a regular compaction.
          // In this case, both clustering and z-order parameters should be empty.
          doAssert(lastOperationParameters(CLUSTERING_PARAMETER_KEY) === "[]")
          doAssert(lastOperationParameters(ZORDER_PARAMETER_KEY) === "[]")
        }
      case o if clusterBySupportedOperations.contains(o) =>
        if (expectClustering) {
          assertClusterByExists()
        } else if (isClusteredTable && clusteringColumns.isEmpty) {
          assertClusterByEmptyOrNotExists()
        } else {
          assertClusterByNotExist()
        }
      case _ =>
        // Other operations are not tested yet. If the test fails here, please check the expected
        // behavior and add the operation to the appropriate case.
        doAssert(false)
    }
  }

  def withClusteredTable[T](
      table: String,
      schema: String,
      clusterBy: String,
      tableProperties: Map[String, String] = Map.empty,
      location: Option[String] = None)(f: => T): T = {
    createOrReplaceClusteredTable("CREATE", table, schema, clusterBy, tableProperties, location)

    Utils.tryWithSafeFinally(f) {
      spark.sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  /**
   * Helper for creating or replacing table with different APIs.
   * @param clause clause for SQL API ('CREATE', 'REPLACE', 'CREATE OR REPLACE')
   * @param table the name of table
   * @param schema comma separated list of "colName dataType"
   * @param clusterBy comma separated list of clustering columns
   */
  def createOrReplaceClusteredTable(
      clause: String,
      table: String,
      schema: String,
      clusterBy: String,
      tableProperties: Map[String, String] = Map.empty,
      location: Option[String] = None): Unit = {
    val locationClause = if (location.isEmpty) "" else s"LOCATION '${location.get}'"
    val tablePropertiesClause = if (!tableProperties.isEmpty) {
      val tablePropertiesString = tableProperties.map {
        case (key, value) => s"'$key' = '$value'"
      }.mkString(",")
      s"TBLPROPERTIES($tablePropertiesString)"
    } else {
      ""
    }
    spark.sql(s"$clause TABLE $table ($schema) USING delta CLUSTER BY ($clusterBy) " +
      s"$tablePropertiesClause $locationClause")
  }

  protected def createOrReplaceAsSelectClusteredTable(
      clause: String,
      table: String,
      srcTable: String,
      clusterBy: String,
      location: Option[String] = None): Unit = {
    val locationClause = if (location.isEmpty) "" else s"LOCATION '${location.get}'"
    spark.sql(s"$clause TABLE $table USING delta CLUSTER BY ($clusterBy) " +
      s"$locationClause AS SELECT * FROM $srcTable")
  }

  def verifyClusteringColumns(
      tableIdentifier: TableIdentifier,
      expectedLogicalClusteringColumns: String): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, tableIdentifier)
    verifyClusteringColumnsInternal(
      snapshot,
      tableIdentifier.table,
      expectedLogicalClusteringColumns
    )
  }

  def verifyClusteringColumns(
      dataPath: String,
      expectedLogicalClusteringColumns: String): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, dataPath)
    verifyClusteringColumnsInternal(
      snapshot,
      s"delta.`$dataPath`",
      expectedLogicalClusteringColumns
    )
  }

  def verifyClusteringColumnsInternal(
      snapshot: Snapshot,
      tableNameOrPath: String,
      expectedLogicalClusteringColumns: String): Unit = {
    assert(ClusteredTableUtils.isSupported(snapshot.protocol) === true)
    verifyClusteringColumnsInDomainMetadata(snapshot, expectedLogicalClusteringColumns)

    // Verify Delta history operation parameters' clusterBy
    verifyDescribeHistoryOperationParameters(tableNameOrPath)
  }
}

trait ClusteredTableTestUtils extends ClusteredTableTestUtilsBase
