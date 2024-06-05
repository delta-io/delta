/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.tables

import scala.collection.JavaConverters._

import io.delta.connect.proto
import io.delta.connect.spark.{proto => spark_proto}

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.connect.delta.ImplicitProtoConversions._

/**
 * Main class for programmatically interacting with Delta tables.
 * You can create DeltaTable instances using the static methods.
 * {{{
 *   DeltaTable.forPath(sparkSession, pathToTheDeltaTable)
 * }}}
 *
 * @since 4.0.0
 */
class DeltaTable private[tables](
    private val df: Dataset[Row],
    private val table: proto.DeltaTable)
  extends Serializable {

  private def sparkSession: SparkSession = df.sparkSession

  /**
   * Apply an alias to the DeltaTable. This is similar to `Dataset.as(alias)` or
   * SQL `tableName AS alias`.
   *
   * @since 4.0.0
   */
  def as(alias: String): DeltaTable = new DeltaTable(df.as(alias), table)

  /**
   * Apply an alias to the DeltaTable. This is similar to `Dataset.as(alias)` or
   * SQL `tableName AS alias`.
   *
   * @since 4.0.0
   */
  def alias(alias: String): DeltaTable = as(alias)

  /**
   * Get a DataFrame (that is, Dataset[Row]) representation of this Delta table.
   *
   * @since 4.0.0
   */
  def toDF: Dataset[Row] = df
}

/**
 * Companion object to create DeltaTable instances.
 *
 * {{{
 *   DeltaTable.forPath(sparkSession, pathToTheDeltaTable)
 * }}}
 *
 * @since 4.0.0
 */
object DeltaTable {
  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path, If the given
   * path is invalid (i.e. either no table exists or an existing table is not a Delta table),
   * it throws a `not a Delta table` error.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @since 4.0.0
   */
  def forPath(path: String): DeltaTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    forPath(sparkSession, path)
  }

  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path, If the given
   * path is invalid (i.e. either no table exists or an existing table is not a Delta table),
   * it throws a `not a Delta table` error.
   *
   * @since 4.0.0
   */
  def forPath(sparkSession: SparkSession, path: String): DeltaTable = {
    forPath(sparkSession, path, Map.empty[String, String])
  }

  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path, If the given
   * path is invalid (i.e. either no table exists or an existing table is not a Delta table),
   * it throws a `not a Delta table` error.
   *
   * @param hadoopConf Hadoop configuration starting with "fs." or "dfs." will be picked up
   *                   by `DeltaTable` to access the file system when executing queries.
   *                   Other configurations will not be allowed.
   *
   * {{{
   *   val hadoopConf = Map(
   *     "fs.s3a.access.key" -> "<access-key>",
   *     "fs.s3a.secret.key" -> "<secret-key>"
   *   )
   *   DeltaTable.forPath(spark, "/path/to/table", hadoopConf)
   * }}}
   *
   * @since 4.0.0
   */
  def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: scala.collection.Map[String, String]): DeltaTable = {
    val table = proto.DeltaTable
      .newBuilder()
      .setPath(
        proto.DeltaTable.Path
          .newBuilder().setPath(path)
          .putAllHadoopConf(hadoopConf.asJava))
      .build()
    forTable(sparkSession, table)
  }

  /**
   * Java friendly API to instantiate a [[DeltaTable]] object representing the data at the given
   * path, If the given path is invalid (i.e. either no table exists or an existing table is not a
   * Delta table), it throws a `not a Delta table` error.
   *
   * @param hadoopConf Hadoop configuration starting with "fs." or "dfs." will be picked up
   *                   by `DeltaTable` to access the file system when executing queries.
   *                   Other configurations will be ignored.
   *
   * {{{
   *   val hadoopConf = Map(
   *     "fs.s3a.access.key" -> "<access-key>",
   *     "fs.s3a.secret.key", "<secret-key>"
   *   )
   *   DeltaTable.forPath(spark, "/path/to/table", hadoopConf)
   * }}}
   *
   * @since 4.0.0
   */
  def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: java.util.Map[String, String]): DeltaTable = {
    val fsOptions = hadoopConf.asScala.toMap
    forPath(sparkSession, path, fsOptions)
  }

  /**
   * Instantiate a [[DeltaTable]] object using the given table name. If the given
   * tableOrViewName is invalid (i.e. either no table exists or an existing table is not a
   * Delta table), it throws a `not a Delta table` error. Note: Passing a view name will also
   * result in this error as views are not supported.
   *
   * The given tableOrViewName can also be the absolute path of a delta datasource (i.e.
   * delta.`path`), If so, instantiate a [[DeltaTable]] object representing the data at
   * the given path (consistent with the [[forPath]]).
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @since 4.0.0
   */
  def forName(tableOrViewName: String): DeltaTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    forName(sparkSession, tableOrViewName)
  }

  /**
   * Instantiate a [[DeltaTable]] object using the given table name using the given
   * SparkSession. If the given tableName is invalid (i.e. either no table exists or an
   * existing table is not a Delta table), it throws a `not a Delta table` error. Note:
   * Passing a view name will also result in this error as views are not supported.
   *
   * The given tableName can also be the absolute path of a delta datasource (i.e.
   * delta.`path`), If so, instantiate a [[DeltaTable]] object representing the data at
   * the given path (consistent with the [[forPath]]).
   *
   * @since 4.0.0
   */
  def forName(sparkSession: SparkSession, tableName: String): DeltaTable = {
    val table = proto.DeltaTable
      .newBuilder()
      .setTableOrViewName(tableName)
      .build()
    forTable(sparkSession, table)
  }

  private def forTable(sparkSession: SparkSession, table: proto.DeltaTable): DeltaTable = {
    val relation = proto.DeltaRelation
      .newBuilder()
      .setScan(proto.Scan.newBuilder().setTable(table))
      .build()
    val extension = com.google.protobuf.Any.pack(relation)
    val sparkRelation = spark_proto.Relation.newBuilder().setExtension(extension).build()
    val df = sparkSession.newDataFrame(_.mergeFrom(sparkRelation))
    new DeltaTable(df, table)
  }
}
