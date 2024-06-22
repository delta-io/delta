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

import org.apache.spark.sql.delta.{DeltaColumnMapping, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{IgnoreCachedData, LeafCommand, LogicalPlan, UnaryCommand}

object DeltaReorgTableMode extends Enumeration {
  val PURGE, UNIFORM_ICEBERG, REWRITE_TYPE_WIDENING = Value
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
 * The REORG TABLE command.
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

  override def optimizeByReorg(sparkSession: SparkSession): Seq[Row] = {
    val command = OptimizeTableCommand(
      target,
      predicates,
      optimizeContext = DeltaOptimizeContext(
        reorg = Some(reorgOperation),
        minFileSize = Some(0L),
        maxDeletedRowsRatio = Some(0d))
    )(zOrderBy = Nil)
    command.run(sparkSession)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = reorgTableSpec match {
    case DeltaReorgTableSpec(
        DeltaReorgTableMode.PURGE | DeltaReorgTableMode.REWRITE_TYPE_WIDENING, None) =>
      optimizeByReorg(sparkSession)
    case DeltaReorgTableSpec(DeltaReorgTableMode.UNIFORM_ICEBERG, Some(icebergCompatVersion)) =>
      val table = getDeltaTable(target, "REORG")
      upgradeUniformIcebergCompatVersion(table, sparkSession, icebergCompatVersion)
  }

  protected def reorgOperation: DeltaReorgOperation = reorgTableSpec match {
    case DeltaReorgTableSpec(DeltaReorgTableMode.PURGE, None) =>
      new DeltaPurgeOperation()
    case DeltaReorgTableSpec(DeltaReorgTableMode.UNIFORM_ICEBERG, Some(icebergCompatVersion)) =>
      new DeltaUpgradeUniformOperation(icebergCompatVersion)
    case DeltaReorgTableSpec(DeltaReorgTableMode.REWRITE_TYPE_WIDENING, None) =>
      new DeltaRewriteTypeWideningOperation()
  }
}

/**
 * Defines a Reorg operation to be applied during optimize.
 */
sealed trait DeltaReorgOperation {
  /**
   * Collects files that need to be processed by the reorg operation from the list of candidate
   * files.
   */
  def filterFilesToReorg(spark: SparkSession, snapshot: Snapshot, files: Seq[AddFile]): Seq[AddFile]
}

/**
 * Reorg operation to purge files with soft deleted rows.
 */
class DeltaPurgeOperation extends DeltaReorgOperation {
  override def filterFilesToReorg(spark: SparkSession, snapshot: Snapshot, files: Seq[AddFile])
    : Seq[AddFile] =
    files.filter { file =>
      (file.deletionVector != null && file.numPhysicalRecords.isEmpty) ||
        file.numDeletedRecords > 0L
    }
}

/**
 * Reorg operation to upgrade the iceberg compatibility version of a table.
 */
class DeltaUpgradeUniformOperation(icebergCompatVersion: Int) extends DeltaReorgOperation {
  override def filterFilesToReorg(spark: SparkSession, snapshot: Snapshot, files: Seq[AddFile])
    : Seq[AddFile] = {
    def shouldRewriteToBeIcebergCompatible(file: AddFile): Boolean = {
      if (file.tags == null) return true
      val icebergCompatVersion = file.tags.getOrElse(AddFile.Tags.ICEBERG_COMPAT_VERSION.name, "0")
      !icebergCompatVersion.exists(_.toString == icebergCompatVersion)
    }
    files.filter(shouldRewriteToBeIcebergCompatible)
  }
}

/**
 * Internal reorg operation to rewrite files to conform to the current table schema when dropping
 * the type widening table feature.
 */
class DeltaRewriteTypeWideningOperation extends DeltaReorgOperation with ReorgTableHelper {
  override def filterFilesToReorg(spark: SparkSession, snapshot: Snapshot, files: Seq[AddFile])
    : Seq[AddFile] = {
    val physicalSchema = DeltaColumnMapping.renameColumns(snapshot.schema)
    filterParquetFilesOnExecutors(spark, files, snapshot, ignoreCorruptFiles = false) {
      schema => fileHasDifferentTypes(schema, physicalSchema)
    }
  }
}
