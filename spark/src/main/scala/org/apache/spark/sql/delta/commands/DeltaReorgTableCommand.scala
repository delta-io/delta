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
import org.apache.spark.sql.delta.sources.DeltaSourceUtils

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.plans.logical.{IgnoreCachedData, LeafCommand, LogicalPlan, UnaryCommand}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

object DeltaReorgTableMode extends Enumeration {
  val PURGE, UNIFORM_ICEBERG = Value
}

case class DeltaReorgTableSpec(
    reorgTableMode: DeltaReorgTableMode.Value,
    icebergCompatVersionOpt: Option[Int]
)

case class DeltaReorgTable(
    target: LogicalPlan,
    reorgTableSpec: DeltaReorgTableSpec = DeltaReorgTableSpec(DeltaReorgTableMode.PURGE, None))(
    val predicates: Seq[String]) extends UnaryCommand {

  def child: LogicalPlan = target

  protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(target = newChild)(predicates)

  override val otherCopyArgs: Seq[AnyRef] = predicates :: Nil
}

/**
 * The PURGE command.
 */
case class DeltaReorgTableCommand(
    target: LogicalPlan,
    reorgTableSpec: DeltaReorgTableSpec = DeltaReorgTableSpec(DeltaReorgTableMode.PURGE, None))(
    val predicates: Seq[String])
  extends OptimizeTableCommandBase
  with ReorgTableForUpgradeUniformHelper
  with LeafCommand
  with IgnoreCachedData {

  override val otherCopyArgs: Seq[AnyRef] = predicates :: Nil

  override def optimizeByReorg(
    sparkSession: SparkSession,
    isPurge: Boolean,
    icebergCompatVersion: Option[Int]): Seq[Row] = {
    val command = OptimizeTableCommand(
      target,
      predicates,
      optimizeContext = DeltaOptimizeContext(
        isPurge = isPurge,
        minFileSize = Some(0L),
        maxDeletedRowsRatio = Some(0d),
        icebergCompatVersion = icebergCompatVersion
      )
    )(zOrderBy = Nil)
    command.run(sparkSession)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    reorgTableSpec match {
      case DeltaReorgTableSpec(DeltaReorgTableMode.PURGE, None) =>
        optimizeByReorg(sparkSession, isPurge = true, icebergCompatVersion = None)
      case DeltaReorgTableSpec(DeltaReorgTableMode.UNIFORM_ICEBERG, Some(icebergCompatVersion)) =>
        val table = getDeltaTable(target, "REORG")
        upgradeUniformIcebergCompatVersion(table, sparkSession, icebergCompatVersion)
    }
  }
}
