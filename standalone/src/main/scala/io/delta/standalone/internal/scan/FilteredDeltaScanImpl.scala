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

package io.delta.standalone.internal.scan

import java.util.Optional

import io.delta.standalone.expressions.Expression
import io.delta.standalone.types.StructType
import io.delta.standalone.internal.actions.AddFile
import io.delta.standalone.internal.data.PartitionRowRecord
import io.delta.standalone.internal.util.PartitionUtils

/**
 * An implementation of [[io.delta.standalone.DeltaScan]] that filters files and only returns
 * those that match the [[getPushedPredicate]].
 *
 * If the pushed predicate is empty, then all files are returned.
 */
final private[internal] class FilteredDeltaScanImpl(
    files: Seq[AddFile],
    expr: Expression,
    partitionSchema: StructType) extends DeltaScanImpl(files) {

  private val partitionColumns = partitionSchema.getFieldNames.toSeq

  private val (metadataConjunction, dataConjunction) =
    PartitionUtils.splitMetadataAndDataPredicates(expr, partitionColumns)

  override protected def accept(addFile: AddFile): Boolean = {
    if (metadataConjunction.isEmpty) return true

    val partitionRowRecord = new PartitionRowRecord(partitionSchema, addFile.partitionValues)
    val result = metadataConjunction.get.eval(partitionRowRecord)
    result.asInstanceOf[Boolean]
  }

  override def getPushedPredicate: Optional[Expression] =
    Optional.ofNullable(metadataConjunction.orNull)

  override def getResidualPredicate: Optional[Expression] =
    Optional.ofNullable(dataConjunction.orNull)

}
