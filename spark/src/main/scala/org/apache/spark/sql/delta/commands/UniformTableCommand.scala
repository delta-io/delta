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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{IgnoreCachedData, LogicalPlan, UnaryNode}
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.execution.command.LeafRunnableCommand

import scala.util.control.NonFatal

case class CreateUniformTableStatement(
    table: LogicalPlan,
    ifNotExists: Boolean,
    isReplace: Boolean,
    isCreate: Boolean,
    fileFormat: String,
    metadataPath: String) extends UnaryNode {

  override def child: LogicalPlan = table

  override def output: Seq[Attribute] = Nil

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

case class RefreshUniformTableStatement(
    target: LogicalPlan,
    isForce: Boolean,
    metadataPath: Option[String]) extends UnaryNode {

  override def child: LogicalPlan = target

  override def output: Seq[Attribute] = Nil

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(target = newChild)
}

case class RefreshUniformTableCommand(
    table: DeltaTableV2,
    isForce: Boolean,
    providedMetadataPath: Option[String])
  extends LeafRunnableCommand
    with DeltaCommand
    with IgnoreCachedData
    with DeltaLogging {

  override val output: Seq[Attribute] = CloneTableCommand.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalogTable = table.catalogTable.getOrElse(
      throw DeltaErrors.notADeltaTable(table.name())
    )

    if (!catalogTable.properties.contains("_isUniformIngressTable")) {
      // TODO: change the error to `UniformIngressNotUFITable`
      throw DeltaErrors.uniformIngressOperationNotSupported
    }

    val icebergMetadataLoc = providedMetadataPath.getOrElse(
      // TODO: change the error to `uniformIngressMetadataPathNotProvided`
      throw DeltaErrors.uniformIngressOperationNotSupported
    )

    // TODO: add sanity check to ensure the `providedMetadataPath` and `table.path`
    // is under the same dir.

    val deltaLog = table.deltaLog
    val snapshot = deltaLog.update()
    val cloneSource = CloneIcebergSource(
      tableIdentifier = TableIdentifier(icebergMetadataLoc),
      sparkTable = None,
      tableSchema = Some(snapshot.metadata.schema),
      spark = sparkSession
    )

    val txn = deltaLog.startTransaction(Some(catalogTable))
    var res = Seq.empty[Row]
    try {
      res = CloneTableCommand(
        sourceTable = cloneSource,
        targetIdent = catalogTable.identifier,
        // TODO: this is a current workaround for bypassing cloning check,
        //  needs further review.
        targetPath = table.path,
        tablePropertyOverrides = Map.empty,
        isUFI = true
      ).handleClone(sparkSession, txn, deltaLog)
    } catch {
      case NonFatal(e) =>
        logError(s"Error convert metadata to delta for table ${catalogTable.identifier} " +
          s"${e.getStackTrace.mkString("\n")}")
        // TODO: change the error to `uniformIngressMetadataConversionFailed`
        throw DeltaErrors.uniformIngressOperationNotSupported
    }
    val updateSnapshot = deltaLog.update()
    logInfo(s"Convert metadata from $icebergMetadataLoc to delta succeed for" +
      s"table ${catalogTable.identifier} with updated version ${updateSnapshot.version}")

    res
  }
}