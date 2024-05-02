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

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Builder class for constructing OPTIMIZE command and executing.
 *
 * @param sparkSession SparkSession to use for execution
 * @param tableIdentifier Id of the table on which to
 *        execute the optimize
 * @param options Hadoop file system options for read and write.
 * @since 2.5.0
 */
class DeltaOptimizeBuilder private(
    private val sparkSession: SparkSession,
    private val table: proto.DeltaTable) {
  private var partitionFilters: Seq[String] = Seq.empty

  /**
   * Apply partition filter on this optimize command builder to limit
   * the operation on selected partitions.
   *
   * @param partitionFilter The partition filter to apply
   * @return [[DeltaOptimizeBuilder]] with partition filter applied
   * @since 2.5.0
   */
  def where(partitionFilter: String): DeltaOptimizeBuilder = {
    this.partitionFilters = this.partitionFilters :+ partitionFilter
    this
  }

  /**
   * Compact the small files in selected partitions.
   *
   * @return DataFrame containing the OPTIMIZE execution metrics
   * @since 2.5.0
   */
  def executeCompaction(): DataFrame = {
    execute(Seq.empty)
  }

  /**
   * Z-Order the data in selected partitions using the given columns.
   *
   * @param columns Zero or more columns to order the data
   *                using Z-Order curves
   * @return DataFrame containing the OPTIMIZE execution metrics
   * @since 2.5.0
   */
  @scala.annotation.varargs
  def executeZOrderBy(columns: String*): DataFrame = {
    execute(columns)
  }

  private def execute(zOrderBy: Seq[String]): DataFrame = {
    val optimize = proto.OptimizeTable
      .newBuilder()
      .setTable(table)
      .addAllPartitionFilters(partitionFilters.asJava)
      .addAllZorderColumns(zOrderBy.asJava)
    val relation = proto.DeltaRelation.newBuilder().setOptimizeTable(optimize).build()
    val extension = com.google.protobuf.Any.pack(relation)
    val result = sparkSession.newDataFrame(extension).collectResult()
    sparkSession.createDataFrame(result.toArray.toSeq.asJava, result.schema)
  }
}

private[delta] object DeltaOptimizeBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      sparkSession: SparkSession,
      table: proto.DeltaTable): DeltaOptimizeBuilder = {
    new DeltaOptimizeBuilder(sparkSession, table)
  }
}
