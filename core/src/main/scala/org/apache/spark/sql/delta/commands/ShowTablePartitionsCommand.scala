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

package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.{DeltaErrors, DeltaTableIdentifier}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * A command lists partitions of a table (optionally add detail(files, modification_ts, size)).
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW PARTITIONS [DETAIL] (db.table|'/path/to/dir'|delta.`/path/to/dir`) [PARTITION(clause)];
 * }}}
 *
 * @param tableIdentifier the identifier of the Delta table
 * @param partitionSpec   partition predicate
 * @param addDetail       add partition details to results
 */
case class ShowTablePartitionsCommand(
  tableIdentifier: DeltaTableIdentifier,
  partitionSpec: Map[String, String],
  addDetail: Boolean
) extends LeafRunnableCommand with DeltaCommand {

  private val (deltaLog, snapshot) = tableIdentifier.getDeltaLogWithSnapshot(SparkSession.active)

  private val partitionSchema = snapshot.metadata.partitionSchema

  private val partitionDetailsField = StructField(
    "partition_detail",
    StructType(
      Seq(
        StructField("files", ArrayType(StringType, containsNull = false), nullable = false),
        StructField("size", LongType, nullable = false),
        StructField("modificationTime", LongType, nullable = false)
      )
    ),
    nullable = false
  )

  private val outputSchema = if (addDetail) {
    partitionSchema.add(partitionDetailsField)
  } else {
    partitionSchema
  }

  override val output: Seq[Attribute] = outputSchema.toAttributes

  override def run(sparkSession: SparkSession): Seq[Row] =
    recordDeltaOperation(deltaLog, "delta.ddl.showPartitions") {

      val partitionColumns = snapshot.metadata.partitionColumns

      if (partitionColumns.isEmpty) {
        throw DeltaErrors.showPartitionInNotPartitionedTable(tableIdentifier.unquotedString)
      }
      val nonPartitionColumns = partitionSpec.keySet.diff(partitionColumns.toSet)
      if (nonPartitionColumns.nonEmpty) {
        throw DeltaErrors.showPartitionInNotPartitionedColumn(nonPartitionColumns)
      }

      val partitionValuesProjection = partitionSchema
        .map { case StructField(name, dataType, _, _) =>
          col(s"partitionValues.$name").cast(dataType).as(name)
        }

      val partitionFilter = partitionSpec
        .map { case (k, v) => expr(s"$k <=> $v") }
        .foldLeft(lit(true))(_ && _)

      withStatusCode("DELTA", s"ShowTablePartitionsCommand: ${tableIdentifier.unquotedString}") {

        val partitionsDf = tableIdentifier
          .getDeltaLog(sparkSession)
          .unsafeVolatileSnapshot
          .allFiles
          .select(
            partitionValuesProjection :+
              col("path") :+
              col("size") :+
              col("modificationTime"): _*)
          .where(partitionFilter)

        if (addDetail) {
          partitionsDf
            .groupBy(partitionColumns.map(col): _*)
            .agg(
              struct(
                collect_set(col("path")).as("files"),
                sum(col("size")).as("size"),
                max(col("modificationTime")).as("modificationTime")
              ).as("partition_detail")
            ).collect()
        } else {
          partitionsDf.select(partitionColumns.map(col): _*).distinct().collect()
        }
      }
    }
}
