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

import org.apache.spark.sql.delta.catalog.DeltaTableV2

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{IgnoreCachedData, LeafCommand, LogicalPlan, UnaryCommand}

case class DeltaReorgTable(target: LogicalPlan)(val predicates: Seq[String]) extends UnaryCommand {

  def child: LogicalPlan = target

  protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(target = newChild)(predicates)

  override val otherCopyArgs: Seq[AnyRef] = predicates :: Nil
}

/**
 * The PURGE command.
 */
case class DeltaReorgTableCommand(target: DeltaTableV2)(val predicates: Seq[String])
  extends OptimizeTableCommandBase with LeafCommand with IgnoreCachedData {

  override val otherCopyArgs: Seq[AnyRef] = predicates :: Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val command = OptimizeTableCommand(
      Option(target.path.toString),
      target.catalogTable.map(_.identifier),
      predicates,
      options = Map.empty,
      optimizeContext = DeltaOptimizeContext(
        isPurge = true,
        minFileSize = Some(0L),
        maxDeletedRowsRatio = Some(0d))
    )(zOrderBy = Nil)
    command.run(sparkSession)
  }
}
