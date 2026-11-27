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

package org.apache.spark.sql.delta.amt

import scala.util.Try

import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.AlterTableSetPropertiesDeltaCommand
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession

/**
 * The downgrade of an AMT table:
 *  1. The pre-downgrade commit sets the AMT property to false. From then on:
 *     - writers write Delta file actions (no back references, no snapshot IDs);
 *     - no commit or checkpoint writes a tree, and checkpoints are classic or V2 again;
 *     - each commit still carries `lastManifestCommit` until the drop commit.
 */
object AMTDowngrade extends DeltaLogging {

  /**
   * Commits the pre-downgrade commit. A rerun of DROP FEATURE finds AMT writes off and commits
   * nothing.
   */
  def commitPreDowngrade(spark: SparkSession, table: DeltaTableV2): Boolean = {
    if (!AMTUtils.amtEnabled(table.update(), isPreviewCompatible = false)) return false
    AlterTableSetPropertiesDeltaCommand(
      table,
      Map(DeltaConfigs.ADAPTIVE_METADATA_ENABLED.key -> "false")).run(spark)
    true
  }

  /**
   * `action` with no AMT fields, and with an `r` DV written as `u` or `p`.
   *
   * A snapshot that Delta builds from a tree has files with back references, snapshot IDs and
   * `r` DVs. A command copies these fields into the actions that it writes, for example into the
   * RemoveFile of a DELETE. The pre-downgrade commit turns AMT writes off, so the commands after
   * it add no AMT fields to new actions. These commands still copy the fields of the snapshot
   * files, and this function removes them.
   */
  def toDeltaForm(action: Action, tableRoot: Path): Action = action match {
    case a: AddFile => toDeltaForm(a, tableRoot)
    case r: RemoveFile => toDeltaForm(r, tableRoot)
    case other => other
  }

  def toDeltaForm(add: AddFile, tableRoot: Path): AddFile = add.copy(
    backReference = None,
    snapshotId = None,
    modifiedSnapshotId = None,
    amtPassthrough = None,
    deletionVector = toClassicDV(add.deletionVector, tableRoot))

  def toDeltaForm(remove: RemoveFile, tableRoot: Path): RemoveFile = remove.copy(
    backReference = None,
    snapshotId = None,
    modifiedSnapshotId = None,
    deletionVector = toClassicDV(remove.deletionVector, tableRoot))

  /**
   * `dv` in a form clients without AMT can read. The AMT writer stores every in-table DV in the
   * tree as `r`, a path relative to the table, and clients without AMT cannot read `r`.
   *  - A DV whose file name holds a UUID becomes `u`.
   *  - Any other DV, for example one that an Iceberg writer named `deletion_vector_iceberg.puffin`,
   *    becomes an in-table `p`. A rewrite of the file under a UUID name needs its own commit, and
   *    that commit races user writes on the same data file. With `p`, the downgrade rewrites no
   *    DV file.
   */
  def toClassicDV(dv: DeletionVectorDescriptor, tableRoot: Path): DeletionVectorDescriptor = {
    if (dv == null || !dv.isUnencodedRelative) return dv
    val relativePath = new Path(dv.pathOrInlineDv)
    Try(DeletionVectorDescriptor.getUUIDFromDeletionVectorFileName(relativePath.getName)).toOption
      .map { uuid =>
        val prefix = Option(relativePath.getParent).map(_.toString).getOrElse("")
        dv.copy(
          storageType = DeletionVectorDescriptor.UUID_DV_MARKER,
          pathOrInlineDv = DeletionVectorDescriptor.encodeUUID(uuid, prefix))
      }
      // The `u` form must name the same file.
      .filter(_.absolutePath(tableRoot) == dv.absolutePath(tableRoot))
      .getOrElse(dv.copyWithAbsolutePath(tableRoot))
  }
}
