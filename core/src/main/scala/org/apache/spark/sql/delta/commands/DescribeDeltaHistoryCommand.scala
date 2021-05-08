/*
 * Copyright (2020) The Delta Lake Project Authors.
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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier}
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.execution.command.RunnableCommand

/**
 * A logical placeholder for describing a Delta table's history, so that the history can be
 * leveraged in subqueries. Replaced with `DescribeDeltaHistoryCommand` during planning.
 */
case class DescribeDeltaHistory(
    path: Option[String],
    tableIdentifier: Option[TableIdentifier],
    limit: Option[Int],
    output: Seq[Attribute] = ExpressionEncoder[CommitInfo]().schema.toAttributes)
  extends LeafNode with MultiInstanceRelation  {
  override def computeStats(): Statistics = Statistics(sizeInBytes = conf.defaultSizeInBytes)

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))
}

/**
 * A command for describing the history of a Delta table.
 */
case class DescribeDeltaHistoryCommand(
    path: Option[String],
    tableIdentifier: Option[TableIdentifier],
    limit: Option[Int],
    override val output: Seq[Attribute] = ExpressionEncoder[CommitInfo]().schema.toAttributes)
  extends RunnableCommand with DeltaLogging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val basePath =
      if (path.nonEmpty) {
        new Path(path.get)
      } else if (tableIdentifier.nonEmpty) {
        val sessionCatalog = sparkSession.sessionState.catalog
        lazy val metadata = sessionCatalog.getTableMetadata(tableIdentifier.get)

        DeltaTableIdentifier(sparkSession, tableIdentifier.get) match {
          case Some(id) if id.path.nonEmpty =>
            new Path(id.path.get)
          case Some(id) if id.table.nonEmpty =>
            new Path(metadata.location)
          case _ =>
            if (metadata.tableType == CatalogTableType.VIEW) {
              throw DeltaErrors.describeViewHistory
            }
            throw DeltaErrors.notADeltaTableException("DESCRIBE HISTORY")
        }
      } else {
        throw DeltaErrors.missingTableIdentifierException("DESCRIBE HISTORY")
      }

    // Max array size
    if (limit.exists(_ > Int.MaxValue - 8)) {
      throw new IllegalArgumentException("Please use a limit less than Int.MaxValue - 8.")
    }

    val deltaLog = DeltaLog.forTable(sparkSession, basePath)
    recordDeltaOperation(deltaLog, "delta.ddl.describeHistory") {
      if (!deltaLog.tableExists) {
        throw DeltaErrors.notADeltaTableException("DESCRIBE HISTORY")
      }

      import sparkSession.implicits._
      deltaLog.history.getHistory(limit).toDF().collect().toSeq
    }
  }

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
}
