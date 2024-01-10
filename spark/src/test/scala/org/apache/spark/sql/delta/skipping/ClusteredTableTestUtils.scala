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

import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumn}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

trait ClusteredTableTestUtilsBase extends SparkFunSuite with SharedSparkSession {
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
      expectedLogicalClusteringColumns: String
    ): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, tableIdentifier)
    verifyClusteringColumnsInternal(
      snapshot,
      tableIdentifier.table,
      expectedLogicalClusteringColumns
    )
  }

  def verifyClusteringColumns(
      dataPath: String,
      expectedLogicalClusteringColumns: String
    ): Unit = {
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
      expectedLogicalClusteringColumns: String
    ): Unit = {
    assert(ClusteredTableUtils.isSupported(snapshot.protocol) === true)
    verifyClusteringColumnsInDomainMetadata(snapshot, expectedLogicalClusteringColumns)
  }
}

trait ClusteredTableTestUtils extends ClusteredTableTestUtilsBase
