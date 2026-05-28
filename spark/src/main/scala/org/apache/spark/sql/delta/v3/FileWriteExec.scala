/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.v3

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.write.PhysicalWriteInfoImpl
import org.apache.spark.sql.connector.write.file.FileWrite
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

/**
 * V2 write exec node base for [[FileWrite]]-backed plans. Holds the child query plan, calls
 * back into [[FileWrite.newFileOutputWriter]] on the driver, and returns an empty result
 * (write commands produce no rows).
 *
 * Replaces the legacy `V1Write` -> `WriteIntoDelta` route used by NONE mode. Subclasses
 * differentiate by the `SaveMode` they pass into the writer.
 */
abstract class FileWriteExec extends V2CommandExec with UnaryExecNode {

  def query: SparkPlan
  def fileWrite: FileWrite
  def saveMode: SaveMode

  override final def child: SparkPlan = query

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val info = PhysicalWriteInfoImpl(child.outputPartitioning.numPartitions)
    fileWrite.newFileOutputWriter(info).write(query, saveMode)
    Nil
  }
}

/**
 * V2 exec node for `AppendData` over a Delta `FileWrite`.
 */
case class AppendFilesExec(query: SparkPlan, fileWrite: FileWrite) extends FileWriteExec {
  override def saveMode: SaveMode = SaveMode.Append
  override protected def withNewChildInternal(newChild: SparkPlan): AppendFilesExec =
    copy(query = newChild)
}

/**
 * V2 exec node for `OverwriteByExpression` over a Delta `FileWrite`. The `deleteExpr`
 * predicate is carried into `FileWrite.newFileOutputWriter` via the write options on
 * [[DeltaFileWrite]] (translated to `replaceWhere`) by [[DeltaWriteBuilder]].
 */
case class OverwriteFilesByExpressionExec(
    query: SparkPlan, fileWrite: FileWrite) extends FileWriteExec {
  override def saveMode: SaveMode = SaveMode.Overwrite
  override protected def withNewChildInternal(
      newChild: SparkPlan): OverwriteFilesByExpressionExec = copy(query = newChild)
}

/**
 * V2 exec node for `OverwritePartitionsDynamic` over a Delta `FileWrite`. The dynamic
 * partition-overwrite mode marker (`PARTITION_OVERWRITE_MODE_DYNAMIC`) is set on the
 * `DeltaFileWrite` options by [[DeltaWriteBuilder.overwriteDynamicPartitions]].
 */
case class OverwritePartitionFilesDynamicExec(
    query: SparkPlan, fileWrite: FileWrite) extends FileWriteExec {
  override def saveMode: SaveMode = SaveMode.Overwrite
  override protected def withNewChildInternal(
      newChild: SparkPlan): OverwritePartitionFilesDynamicExec = copy(query = newChild)
}
