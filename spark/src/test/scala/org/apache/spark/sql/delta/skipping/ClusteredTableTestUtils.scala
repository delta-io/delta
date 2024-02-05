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
import org.apache.spark.sql.delta.DeltaOperations.CLUSTERING_PARAMETER_KEY
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics

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
    val deltaLog = if (table.startsWith("tahoe.") || table.startsWith("delta.")) {
      // Path based table.
      DeltaLog.forTable(spark, table.drop(6).replace("`", ""))
    } else {
      DeltaLog.forTable(spark, TableIdentifier(table))
    }
    val clusteringColumns =
      ClusteringColumnInfo.extractLogicalNames(deltaLog.unsafeVolatileSnapshot)
    val lastEvent = deltaLog.history.getHistory(Some(1)).head
    lastEvent.operation match {
      case "CLUSTER BY" =>
        // Operation is [[DeltaOperations.ClusterBy]] - ALTER TABLE CLUSTER BY
        assert(
          lastEvent.operationParameters("newClusteringColumns") === clusteringColumns.mkString(",")
        )
      case "CREATE TABLE" | "REPLACE TABLE" | "CREATE OR REPLACE TABLE" | "CREATE TABLE AS SELECT" |
          "REPLACE TABLE AS SELECT" | "CREATE OR REPLACE TABLE AS SELECT" =>
        val expectedClusterBy = clusteringColumns.mkString("""["""", """","""", """"]""")
        assert(lastEvent.operationParameters(CLUSTERING_PARAMETER_KEY) === expectedClusterBy)
      case _ =>
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
      expectedLogicalClusteringColumns: String,
      verifyDescribeHistory: Boolean = true): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, tableIdentifier)
    verifyClusteringColumnsInternal(
      snapshot,
      tableIdentifier.table,
      expectedLogicalClusteringColumns,
      verifyDescribeHistory
    )
  }

  def verifyClusteringColumns(
      dataPath: String,
      expectedLogicalClusteringColumns: String,
      verifyDescribeHistory: Boolean): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, dataPath)
    verifyClusteringColumnsInternal(
      snapshot,
      s"delta.`$dataPath`",
      expectedLogicalClusteringColumns,
      verifyDescribeHistory
    )
  }

  def verifyClusteringColumnsInternal(
      snapshot: Snapshot,
      tableNameOrPath: String,
      expectedLogicalClusteringColumns: String,
      verifyDescribeHistory: Boolean): Unit = {
    assert(ClusteredTableUtils.isSupported(snapshot.protocol) === true)
    verifyClusteringColumnsInDomainMetadata(snapshot, expectedLogicalClusteringColumns)

    // Verify Delta history operation parameters' clusterBy
    if (verifyDescribeHistory) {
      verifyDescribeHistoryOperationParameters(tableNameOrPath)
    }
  }
}

trait ClusteredTableTestUtils extends ClusteredTableTestUtilsBase
