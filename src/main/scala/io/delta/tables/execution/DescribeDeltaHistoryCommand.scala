/*
 * Copyright 2019 Databricks, Inc.
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

package io.delta.tables.execution

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier}
import org.apache.spark.sql.execution.command.RunnableCommand

case class DescribeDeltaHistoryCommand(
    path: Option[String],
    table: Option[TableIdentifier],
    limit: Option[Int]) extends RunnableCommand {

  override val output: Seq[Attribute] = Encoders.product[CommitInfo].schema
    .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val basePath =
      new Path(if (table.nonEmpty) {
        DeltaTableIdentifier(sparkSession, table.get) match {
          case Some(id) if id.path.isDefined => id.path.get
          case Some(id) => throw DeltaErrors.tableNotSupportedException("DESCRIBE HISTORY")
          case None => throw DeltaErrors.notADeltaTableException("DESCRIBE HISTORY")
        }
      } else {
        path.get
      })

    // Max array size
    if (limit.exists(_ > Int.MaxValue - 8)) {
      throw new IllegalArgumentException("Please use a limit less than Int.MaxValue - 8.")
    }

    val deltaLog = DeltaLog.forTable(sparkSession, basePath)
    if (deltaLog.snapshot.version == -1) {
      throw DeltaErrors.notADeltaTableException(
        "DESCRIBE HISTORY",
        DeltaTableIdentifier(path = Some(basePath.toString)))
    }

    import sparkSession.implicits._
    deltaLog.history.getHistory(limit).toDF().collect().toSeq
  }
}
