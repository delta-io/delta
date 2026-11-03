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

package org.apache.spark.sql.delta.test

import java.io.File
import java.util.UUID

import scala.util.Random

import org.apache.spark.sql.delta.{CatalogOwnedTableFeature, DeltaColumnMappingTestUtilsBase, DeltaLog, DeltaTable, Snapshot, TableFeature}
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.coordinatedcommits.{CatalogOwnedCommitCoordinatorProvider, CatalogOwnedTableUtils, TrackingInMemoryCommitCoordinatorBuilder}
import org.apache.spark.sql.delta.stats.{DeltaStatistics, PreparedDeltaFileIndex}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

trait DeltaSQLTestUtils extends SQLTestUtils {
  /**
   * Override the temp dir/path creation methods from [[SQLTestUtils]] to:
   * 1. Drop the call to `waitForTasksToFinish` which is a source of flakiness due to timeouts
   *    without clear benefits.
   * 2. Allow creating paths with special characters for better test coverage.
   */

  protected val defaultTempDirPrefix: String = "spark%dir%prefix"

  override protected def withTempDir(f: File => Unit): Unit = {
    withTempDir(prefix = defaultTempDirPrefix)(f)
  }

  override protected def withTempPaths(numPaths: Int)(f: Seq[File] => Unit): Unit = {
    withTempPaths(numPaths, prefix = defaultTempDirPrefix)(f)
  }

  override def withTempPath(f: File => Unit): Unit = {
    withTempPath(prefix = defaultTempDirPrefix)(f)
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  def withTempDir(prefix: String)(f: File => Unit): Unit = {
    val path = Utils.createTempDir(namePrefix = prefix)
    try f(path) finally Utils.deleteRecursively(path)
  }

  /**
   * Generates a temporary directory path without creating the actual directory, which is then
   * passed to `f` and will be deleted after `f` returns.
   */
  def withTempPath(prefix: String)(f: File => Unit): Unit = {
    val path = Utils.createTempDir(namePrefix = prefix)
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  /**
   * Generates the specified number of temporary directory paths without creating the actual
   * directories, which are then passed to `f` and will be deleted after `f` returns.
   */
  protected def withTempPaths(numPaths: Int, prefix: String)(f: Seq[File] => Unit): Unit = {
    val files =
      Seq.fill[File](numPaths)(Utils.createTempDir(namePrefix = prefix).getCanonicalFile)
    files.foreach(_.delete())
    try f(files) finally {
      files.foreach(Utils.deleteRecursively)
    }
  }

  /**
   * Creates a temporary table with a unique name for testing and executes a function with it.
   * The table is automatically cleaned up after the function completes.
   *
   * @param createTable Whether to create an empty table.
   * @param f The function to execute with the generated table name.
   */
  protected def withTempTable(createTable: Boolean)(f: String => Unit): Unit = {
    val tableName = s"test_table_${UUID.randomUUID().toString.filterNot(_ == '-')}"

    withTable(tableName) {
      if (createTable) {
        spark.sql(s"CREATE TABLE $tableName (id LONG) USING delta")
      }
      f(tableName)
    }
  }

  /**
   * Creates a Catalog-Managed Delta table for tests.
   *
   * @param createTable Whether to create the table with CatalogOwnedTableFeature enabled.
   * @param f The function to execute with the generated table name.
   */
  protected def withCatalogManagedTable(createTable: Boolean = true)(f: String => Unit): Unit = {
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
    CatalogOwnedCommitCoordinatorProvider.registerBuilder(
      CatalogOwnedTableUtils.DEFAULT_CATALOG_NAME_FOR_TESTING,
      TrackingInMemoryCommitCoordinatorBuilder(batchSize = 3))
    withTempTable(createTable = false) { tableName =>
      if (createTable) {
        spark.sql(s"CREATE TABLE $tableName (id INT) USING delta TBLPROPERTIES " +
          s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")
      }
      f(tableName)
    }
  }

  /** Returns random alphanumberic string to be used as a unique table name. */
  def uniqueTableName: String = Random.alphanumeric.take(10).mkString

  /** Gets the latest snapshot of the table. */
  def getSnapshot(tableName: String): Snapshot = {
    DeltaLog.forTable(spark, TableIdentifier(tableName)).update()
  }

  /** Gets the table protocol of the latest snapshot. */
  def getProtocolForTable(tableName: String): Protocol = {
    getSnapshot(tableName).protocol
  }
  /** Gets the `StructField` of `columnPath`. */
  final def getColumnField(schema: StructType, columnPath: Seq[String]): StructField = {
    schema.findNestedField(columnPath, includeCollections = true).get._2
  }

  /** Gets the `StructField` of `columnName`. */
  def getColumnField(tableName: String, columnName: String): StructField = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
    getColumnField(deltaLog.update().schema, columnName.split("\\."))
  }

  /** Gets the `DataType` of `columnPath`. */
  def getColumnType(schema: StructType, columnPath: Seq[String]): DataType = {
    getColumnField(schema, columnPath).dataType
  }

  /** Gets the `DataType` of `columnName`. */
  def getColumnType(tableName: String, columnName: String): DataType = {
    getColumnField(tableName, columnName).dataType
  }

  /**
   * Gets the stats fields from the AddFiles of `snapshot`. The stats are ordered by the
   * modification time of the files they are associated with.
   */
  def getUnvalidatedStatsOrderByFileModTime(snapshot: Snapshot): Array[JsonNode] = {
    snapshot.allFiles
      .orderBy("modificationTime")
      .collect()
      .map(file => new ObjectMapper().readTree(file.stats))
  }

  /**
   * Gets the stats fields from the AddFiles of `tableName`. The stats are ordered by the
   * modification time of the files they are associated with.
   */
  def getUnvalidatedStatsOrderByFileModTime(tableName: String): Array[JsonNode] =
    getUnvalidatedStatsOrderByFileModTime(getSnapshot(tableName))

  /** Gets the physical column path if there is column mapping metadata in the schema. */
  def getPhysicalColumnPath(tableSchema: StructType, columnName: String): Seq[String] = {
    new DeltaColumnMappingTestUtilsBase {}.getPhysicalPathForStats(
      columnName.split("\\."), tableSchema
    ).get
  }

  /** Gets the value of a specified field from `stats` JSON node if it exists. */
  def getStatFieldOpt(stats: JsonNode, path: Seq[String]): Option[JsonNode] =
    path.foldLeft(Option(stats)) {
      case (Some(node), key) if node.has(key) => Option(node.get(key))
      case _ => None
    }

  /** Gets the min/max stats of `columnName` from `stats` if they exist. */
  private def getMinMaxStatsOpt(
      tableName: String,
      stats: JsonNode,
      columnName: String): (Option[String], Option[String]) = {
    val schema = getSnapshot(tableName).schema

    val physicalColumnPath = getPhysicalColumnPath(schema, columnName)
    val minStatsPath = DeltaStatistics.MIN +: physicalColumnPath
    val maxStatsPath = DeltaStatistics.MAX +: physicalColumnPath
    (
      getStatFieldOpt(stats, minStatsPath).map(_.asText()),
      getStatFieldOpt(stats, maxStatsPath).map(_.asText()))
  }

  /** Gets the min/max stats of `columnName` from `stats`. */
  def getMinMaxStats(
      tableName: String,
      stats: JsonNode,
      columnName: String): (String, String) = {
    val (minOpt, maxOpt) = getMinMaxStatsOpt(tableName, stats, columnName)
    (minOpt.get, maxOpt.get)
  }

  /** Verifies whether there are min/max stats of `columnName` in `stats`. */
  def assertMinMaxStatsPresence(
      tableName: String,
      stats: JsonNode,
      columnName: String,
      expectStats: Boolean): Unit = {
    val (minStats, maxStats) = getMinMaxStatsOpt(tableName, stats, columnName)
    assert(minStats.isDefined === expectStats)
    assert(maxStats.isDefined === expectStats)
  }

  /** Verifies min/max stats values of `columnName` in `stats`. */
  def assertMinMaxStats(
      tableName: String,
      stats: JsonNode,
      columnName: String,
      expectedMin: String,
      expectedMax: String): Unit = {
    val (min, max) =
      getMinMaxStats(tableName, stats, columnName)
    assert(min === expectedMin, s"Expected $expectedMin, got $min")
    assert(max === expectedMax, s"Expected $expectedMax, got $max")
  }

  /** Verifies minReaderVersion and minWriterVersion of the protocol. */
  def assertProtocolVersion(
      protocol: Protocol,
      minReaderVersion: Int,
      minWriterVersion: Int): Unit = {
    assert(protocol.minReaderVersion === minReaderVersion)
    assert(protocol.minWriterVersion === minWriterVersion)
  }

  /** Verifies column is of expected data type. */
  def assertColumnDataType(
      tableName: String,
      columnName: String,
      expectedDataType: DataType): Unit = {
    assert(getColumnType(tableName, columnName) === expectedDataType)
  }

  /** Verifies `columnName` does not exist in `tableName`. */
  def assertColumnNotExist(tableName: String, columnName: String): Unit = {
    val e = intercept[AnalysisException] {
      sql(s"SELECT $columnName FROM $tableName")
    }
    assert(e.getMessage.contains(s"`$columnName` cannot be resolved"))
  }

  /**
   * Runs `select` query on `tableName` with `predicate` and verifies the number of rows returned
   * and files read.
   */
  def assertSelectQueryResults(
      tableName: String,
      predicate: String,
      numRows: Int,
      numFilesRead: Int): Unit = {
    val query = sql(s"SELECT * FROM $tableName WHERE $predicate")
    assertSelectQueryResults(query, numRows, numFilesRead)
  }

  /**
   * Runs `query` and verifies the number of rows returned
   * and files read.
   */
  def assertSelectQueryResults(
      query: DataFrame,
      numRows: Int,
      numFilesRead: Int): Unit = {
    assert(query.count() === numRows, s"Expected $numRows rows, got ${query.count()}")
    val filesRead = getNumReadFiles(query)
    assert(filesRead === numFilesRead, s"Expected $numFilesRead files read, got $filesRead")
  }

  /** Returns the number of read files by the query with given query text. */
  def getNumReadFiles(queryText: String): Int = {
    getNumReadFiles(sql(queryText))
  }

  /** Returns the number of read files by the given data frame query. */
  def getNumReadFiles(df: DataFrame): Int = {
    val deltaScans = df.queryExecution.optimizedPlan.collect {
      case DeltaTable(prepared: PreparedDeltaFileIndex) => prepared.preparedScan
    }
    assert(deltaScans.size == 1)
    deltaScans.head.files.length
  }

  /** Drops `columnName` from `tableName`. */
  def dropColumn(tableName: String, columnName: String): Unit = {
    sql(s"ALTER TABLE $tableName DROP COLUMN $columnName")
    assertColumnNotExist(tableName, columnName)
  }

  /** Changes `columnName` to `newType` */
  def alterColumnType(tableName: String, columnName: String, newType: String): Unit = {
    sql(s"ALTER TABLE $tableName ALTER COLUMN $columnName TYPE $newType")
  }

  /** Whether the table protocol supports the given table feature. */
  def isFeatureSupported(tableName: String, tableFeature: TableFeature): Boolean = {
    val protocol = getProtocolForTable(tableName)
    protocol.isFeatureSupported(tableFeature)
  }

  /** Whether the table protocol supports the given table feature. */
  def isFeatureSupported(tableName: String, featureName: String): Boolean = {
    val protocol = getProtocolForTable(tableName)
    protocol.readerFeatureNames.contains(featureName) ||
      protocol.writerFeatureNames.contains(featureName)
  }

  /** Enables table feature for `tableName` and given `featureName`. */
  def enableTableFeature(tableName: String, featureName: String): Unit = {
    sql(s"""
           |ALTER TABLE $tableName
           |SET TBLPROPERTIES('delta.feature.$featureName' = 'supported')
           |""".stripMargin)
    assert(isFeatureSupported(tableName, featureName))
  }

  /** Drops table feature for `tableName` and `featureName`. */
  def dropTableFeature(tableName: String, featureName: String): Unit = {
    sql(s"ALTER TABLE $tableName DROP FEATURE `$featureName`")
    assert(!isFeatureSupported(tableName, featureName))
  }
}
