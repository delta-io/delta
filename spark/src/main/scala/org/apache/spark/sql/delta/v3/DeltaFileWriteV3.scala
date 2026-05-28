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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.write.PhysicalWriteInfo
import org.apache.spark.sql.connector.write.file.{FileOutputWriter, FileWrite}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.StructType

/**
 * Delta v3 concrete [[FileWrite]]. Carries the write mode + option set produced by
 * [[DeltaWriteBuilderV3]] through analysis/optimization. At planning time the
 * [[DeltaFileWriteStrategy]] matches the enclosing V2 write command and constructs the
 * corresponding `*FilesExec`; the exec node calls back into [[newFileOutputWriter]] on the
 * driver to drive the actual commit.
 *
 * Constraints and generated-column expressions are not enforced via this FileWrite in the
 * initial implementation - the underlying [[DeltaFileOutputWriterV3]] still delegates to
 * `WriteIntoDelta`, which enforces them inside `TransactionalWrite.writeFiles`. A follow-up
 * will lift enforcement into the V2 exec node itself, exposing the expressions here.
 */
case class DeltaFileWriteV3(
    deltaTable: DeltaTableV3,
    writeOptions: Map[String, String],
    isOverwrite: Boolean,
    isByName: Boolean) extends FileWrite {

  override def writeSchema(): StructType = deltaTable.schema()

  override def fileFormat(): FileFormat = deltaTable.deltaLog.fileFormat(
    deltaTable.initialSnapshot.protocol, deltaTable.initialSnapshot.metadata)

  override def partitionColumns(): Seq[String] =
    deltaTable.initialSnapshot.metadata.partitionColumns

  // Enforcement is currently inside WriteIntoDelta; surface as empty until we lift it
  // into the V2 exec nodes.
  override def constraints(): Seq[Expression] = Seq.empty
  override def generatedColumnExpressions(): Map[String, Expression] = Map.empty

  override def newFileOutputWriter(info: PhysicalWriteInfo): FileOutputWriter =
    new DeltaFileOutputWriterV3(deltaTable, writeOptions, isOverwrite)

  override def description(): String = {
    val mode = if (isOverwrite) "Overwrite" else "Append"
    s"DeltaFileWriteV3($mode, table=${deltaTable.name()})"
  }
}
