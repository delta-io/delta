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
import io.delta.connect.ImplicitProtoConversions._

/**
 * Main class for programmatically interacting with Delta tables.
 * You can create DeltaTable instances using the static methods.
 * {{{
 *   DeltaTable.forPath(sparkSession, pathToTheDeltaTable)
 * }}}
 *
 * @since 0.3.0
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
   * @since 2.4.0
   */
  def as(alias: String): DeltaTable = new DeltaTable(df.as(alias), table)

  /**
   * Apply an alias to the DeltaTable. This is similar to `Dataset.as(alias)` or
   * SQL `tableName AS alias`.
   *
   * @since 2.4.0
   */
  def alias(alias: String): DeltaTable = as(alias)

  /**
   * Get a DataFrame (that is, Dataset[Row]) representation of this Delta table.
   *
   * @since 2.4.0
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
 * @since 2.4.0
 */
object DeltaTable {
  def forPath(path: String): DeltaTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    forPath(sparkSession, path)
  }

  def forPath(sparkSession: SparkSession, path: String): DeltaTable = {
    forPath(sparkSession, path, Map.empty[String, String])
  }

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

  def forPath(
               sparkSession: SparkSession,
               path: String,
               hadoopConf: java.util.Map[String, String]): DeltaTable = {
    val fsOptions = hadoopConf.asScala.toMap
    forPath(sparkSession, path, fsOptions)
  }

  def forName(tableOrViewName: String): DeltaTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    forName(sparkSession, tableOrViewName)
  }

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
