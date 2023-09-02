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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaErrors, DeltaHistory, DeltaLog, UnresolvedDeltaPathOrIdentifier, UnresolvedPathBasedDeltaTable}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{ScalaReflection, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedTable}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.UnaryRunnableCommand
import org.apache.spark.sql.types.StructType

object DescribeDeltaHistory {
  /**
   * Alternate constructor that converts a provided path or table identifier into the
   * correct child LogicalPlan node. If both path and tableIdentifier are specified (or
   * if both are None), this method will throw an exception. If a table identifier is
   * specified, the child LogicalPlan will be an [[UnresolvedTable]] whereas if a path
   * is specified, it will be an [[UnresolvedPathBasedDeltaTable]].
   *
   * Note that the returned command will have an *unresolved* child table and hence, the command
   * needs to be analyzed before it can be executed.
   */
  def apply(
      path: Option[String],
      tableIdentifier: Option[TableIdentifier],
      limit: Option[Int],
      unusedOptions: Map[Nothing, Nothing],
      output: Seq[Attribute] = schema.toAttributes
    ): DescribeDeltaHistoryCommand = {
    val plan = UnresolvedDeltaPathOrIdentifier(path, tableIdentifier, commandName)
    DescribeDeltaHistoryCommand(plan, limit, Map.empty[String, String])
  }

  val schema = ScalaReflection.schemaFor[DeltaHistory].dataType.asInstanceOf[StructType]
  val commandName = "DESCRIBE HISTORY"
}

object DescribeDeltaHistoryCommand {
  /** Same as above, but for the DescribeDeltaHistoryCommand class instead. */
  def apply(
    path: Option[String],
    tableIdentifier: Option[TableIdentifier],
    limit: Option[Int],
    unusedOptions: Map[Nothing, Nothing],
    output: Seq[Attribute]
  ): DescribeDeltaHistoryCommand = {
    val plan = UnresolvedDeltaPathOrIdentifier(
      path, tableIdentifier, DescribeDeltaHistory.commandName)
    DescribeDeltaHistoryCommand(plan, limit)
  }
}

/**
 * A logical placeholder for describing a Delta table's history. Currently unused, in the future
 * this may be used so that the history can be leveraged in subqueries by replacing with
 * `DescribeDeltaHistoryCommand` during planning.
 */
case class DescribeDeltaHistory(
    path: Option[String],
    tableIdentifier: Option[TableIdentifier],
    limit: Option[Int],
    unusedOptions: Map[Nothing, Nothing],
    output: Seq[Attribute])
  extends LeafNode with MultiInstanceRelation {
  override def computeStats(): Statistics = Statistics(sizeInBytes = conf.defaultSizeInBytes)
  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))
}

/**
 * A command for describing the history of a Delta table.
 *
 * @param options: Hadoop file system options used for read and write.
 */
case class DescribeDeltaHistoryCommand(
    override val child: LogicalPlan,
    limit: Option[Int],
    options: Map[String, String] = Map.empty,
    override val output: Seq[Attribute] = DescribeDeltaHistory.schema.toAttributes)
  extends RunnableCommand with UnaryLike[LogicalPlan] with DeltaCommand {

  override protected def withNewChildInternal(newChild: LogicalPlan): DescribeDeltaHistoryCommand =
    copy(child = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    import DescribeDeltaHistory.commandName
    val tableMetadata: Option[CatalogTable] = getTableCatalogTable(child)
    val path = getTablePathOrIdentifier(child, commandName)._2
    val basePath = tableMetadata match {
      case Some(metadata) =>
        new Path(metadata.location)
      case _ if path.isDefined => new Path(path.get)
      case _ => throw DeltaErrors.missingTableIdentifierException(commandName)
    }

    // Max array size
    if (limit.exists(_ > Int.MaxValue - 8)) {
      throw DeltaErrors.maxArraySizeExceeded()
    }

    val deltaLog = DeltaLog.forTable(sparkSession, basePath, options)
    recordDeltaOperation(deltaLog, "delta.ddl.describeHistory") {
      if (!deltaLog.tableExists) {
        throw DeltaErrors.notADeltaTableException(commandName)
      }
      import org.apache.spark.sql.delta.implicits._
      val commits = deltaLog.history.getHistory(limit)
      sparkSession.implicits.localSeqToDatasetHolder(commits).toDF().collect().toSeq
    }
  }
}
