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
import org.apache.spark.sql.delta.{DeltaErrors, DeltaHistory, DeltaLog, DeltaTableIdentifier, UnresolvedDeltaPathOrIdentifier, UnresolvedPathBasedDeltaTable}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedTable}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.command.LeafRunnableCommand

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
      limit: Option[Int]): DescribeDeltaHistory = {
    val plan = UnresolvedDeltaPathOrIdentifier(path, tableIdentifier, COMMAND_NAME)
    DescribeDeltaHistory(plan, limit)
  }

  val COMMAND_NAME = "DESCRIBE HISTORY"
}

/**
 * A logical placeholder for describing a Delta table's history, so that the history can be
 * leveraged in subqueries. Replaced with `DescribeDeltaHistoryCommand` during planning.
 *
 * @param options: Hadoop file system options used for read and write.
 */
case class DescribeDeltaHistory(
    override val child: LogicalPlan,
    limit: Option[Int],
    override val output: Seq[Attribute] = toAttributes(ExpressionEncoder[DeltaHistory]().schema))
  extends UnaryNode
    with MultiInstanceRelation
    with DeltaCommand {

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(child = newChild)

  /**
   * Define this operator as having no attributes provided by children in order to prevent column
   * pruning from trying to insert projections above the source relation.
   */
  override lazy val references: AttributeSet = AttributeSet.empty
  override def inputSet: AttributeSet = AttributeSet.empty
  assert(!child.isInstanceOf[Project],
    s"The child operator of DescribeDeltaHistory must not contain any projection: $child")

  /** Converts this operator into an executable command. */
  def toCommand: DescribeDeltaHistoryCommand = {
    // Max array size
    if (limit.exists(_ > Int.MaxValue - 8)) {
      throw DeltaErrors.maxArraySizeExceeded()
    }
    val deltaTableV2: DeltaTableV2 = getDeltaTable(child, DescribeDeltaHistory.COMMAND_NAME)
    DescribeDeltaHistoryCommand(table = deltaTableV2, limit = limit, output = output)
  }
}

/**
 * A command for describing the history of a Delta table.
 */
case class DescribeDeltaHistoryCommand(
    @transient table: DeltaTableV2,
    limit: Option[Int],
    override val output: Seq[Attribute] = toAttributes(ExpressionEncoder[DeltaHistory]().schema))
  extends LeafRunnableCommand
    with MultiInstanceRelation
    with DeltaLogging {

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.describeHistory") {
      if (!deltaLog.tableExists) {
        throw DeltaErrors.notADeltaTableException(
          DescribeDeltaHistory.COMMAND_NAME,
          DeltaTableIdentifier(path = Some(table.path.toString))
        )
      }
      import org.apache.spark.sql.delta.implicits._
      val commits = deltaLog.history.getHistory(limit)
      sparkSession.implicits.localSeqToDatasetHolder(commits).toDF().collect().toSeq
    }
  }
}
