/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration

import io.delta.standalone.expressions.Expression
import io.delta.standalone.types.StructType

import io.delta.standalone.internal.actions.{AddFile, MemoryOptimizedLogReplay}
import io.delta.standalone.internal.data.PartitionRowRecord
import io.delta.standalone.internal.sources.StandaloneHadoopConf
import io.delta.standalone.internal.util.PartitionUtils

/**
 * An implementation of [[io.delta.standalone.DeltaScan]] that filters files and only returns
 * those that match the [[getPushedPredicate]].
 *
 * If the pushed predicate is empty, then all files are returned.
 */
final private[internal] class FilteredDeltaScanImpl(
    replay: MemoryOptimizedLogReplay,
    expr: Expression,
    partitionSchema: StructType,
    hadoopConf: Configuration) extends DeltaScanImpl(replay) {

  private val partitionColumns = partitionSchema.getFieldNames.toSeq
  private val evaluationResults = mutable.Map.empty[Map[String, String], Boolean]

  private val (metadataConjunction, dataConjunction) =
    PartitionUtils.splitMetadataAndDataPredicates(expr, partitionColumns)

  private val partitionFilterRecordCachingEnabled = hadoopConf
    .getBoolean(StandaloneHadoopConf.PARTITION_FILTER_RECORD_CACHING_KEY, true)

  override protected def accept(addFile: AddFile): Boolean = {
    if (metadataConjunction.isEmpty) return true

    // found in micro-benchmarking that eagerly creating
    // new PartitionRowRecord can destroy the purpose of caching
    lazy val partitionRowRecord = new PartitionRowRecord(partitionSchema, addFile.partitionValues)

    if (partitionFilterRecordCachingEnabled) {
      val cachedResult = evaluationResults.get(addFile.partitionValues)
      if (cachedResult.isDefined) return cachedResult.get
      val result = metadataConjunction.get.eval(partitionRowRecord).asInstanceOf[Boolean]
      evaluationResults(addFile.partitionValues) = result
      result
    } else {
      val result = metadataConjunction.get.eval(partitionRowRecord).asInstanceOf[Boolean]
      result
    }
  }

  override def getInputPredicate: Optional[Expression] = Optional.of(expr)

  override def getPushedPredicate: Optional[Expression] =
    Optional.ofNullable(metadataConjunction.orNull)

  override def getResidualPredicate: Optional[Expression] =
    Optional.ofNullable(dataConjunction.orNull)

}
