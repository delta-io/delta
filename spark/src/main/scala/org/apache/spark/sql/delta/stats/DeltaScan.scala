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

package org.apache.spark.sql.delta.stats

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.stats.DeltaDataSkippingType.DeltaDataSkippingType
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructType

/**
 * DataSize describes following attributes for data that consists of a list of input files
 * @param bytesCompressed total size of the data
 * @param rows number of rows in the data
 * @param files number of input files
 * Note: Please don't add any new constructor to this class. `jackson-module-scala` always picks up
 * the first constructor returned by `Class.getConstructors` but the order of the constructors list
 * is non-deterministic. (SC-13343)
 */
case class DataSize(
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    bytesCompressed: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    rows: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    files: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    logicalRows: Option[Long] = None
)

object DataSize {
  def apply(a: ArrayAccumulator): DataSize = {
    DataSize(
      Option(a.value(0)).filterNot(_ == -1),
      Option(a.value(1)).filterNot(_ == -1),
      Option(a.value(2)).filterNot(_ == -1),
      Option(a.value(3)).filterNot(_ == -1)
    )
  }
}

object DeltaDataSkippingType extends Enumeration {
  type DeltaDataSkippingType = Value
  // V1: code path in DataSkippingReader.scala, which needs StateReconstruction
  // noSkipping: no skipping and get all files from the Delta table
  // partitionFiltering: filtering and skipping based on partition columns
  // dataSkipping: filtering and skipping based on stats columns
  // limit: skipping based on limit clause in DataSkippingReader.scala
  // filteredLimit: skipping based on limit clause and partition columns in DataSkippingReader.scala
  val noSkippingV1, noSkippingV2, partitionFilteringOnlyV1, partitionFilteringOnlyV2,
    dataSkippingOnlyV1, dataSkippingOnlyV2, dataSkippingAndPartitionFilteringV1,
    dataSkippingAndPartitionFilteringV2, limit, filteredLimit = Value
}

/**
 * Used to hold details the files and stats for a scan where we have already
 * applied filters and a limit.
 */
case class DeltaScan(
    version: Long,
    files: Seq[AddFile],
    total: DataSize,
    partition: DataSize,
    scanned: DataSize)(
    // Moved to separate argument list, to not be part of case class equals check -
    // expressions can differ by exprId or ordering, but as long as same files are scanned, the
    // PreparedDeltaFileIndex and HadoopFsRelation should be considered equal for reuse purposes.
    val scannedSnapshot: Snapshot,
    val partitionFilters: ExpressionSet,
    val dataFilters: ExpressionSet,
    val unusedFilters: ExpressionSet,
    val scanDurationMs: Long,
    val dataSkippingType: DeltaDataSkippingType) {
  assert(version == scannedSnapshot.version)

  lazy val filtersUsedForSkipping: ExpressionSet = partitionFilters ++ dataFilters
  lazy val allFilters: ExpressionSet = filtersUsedForSkipping ++ unusedFilters

  /**
   * Compare a set of filters to the filters for this DeltaScan. Because these filters could
   * be post optimization, nested fields may have different schemas due to schema pruning. To
   * get around this, we convert any nested field to an UnresolvedAttribute for the comparison.
   *
   * @param other ExpressionSet to compare the filters against
   * @param resolver resolver used to transform prepared filters to attributes in other
   * @return Whether the expressions match with nested schemas ignored
   */
  def filtersMatch(other: ExpressionSet, resolver: Resolver): Boolean = {
    DeltaScan.filtersMatch(allFilters, other, resolver) ||
      DeltaScan.filtersMatch(filtersUsedForSkipping, other, resolver)
  }
}

object DeltaScan {
  private[delta] def filtersMatch(source: ExpressionSet, target: ExpressionSet,
      resolver: Resolver): Boolean = {
    // Create a map of exprId -> Attribute from target expressions
    val targetAttrs = target.flatMap { e =>
      e.collect { case a: Attribute => a.exprId -> a }
    }.toMap

    // Convert all GetStructField expressions to UnresolvedExtractValue
    val unresolvedSource = source.map(_ transform {
      case g: GetStructField =>
        UnresolvedExtractValue(g.child, Literal(g.extractFieldName))
    })

    // Re-resolve extract values on new attributes
    val resolvedSource = unresolvedSource.map(_ transformUp {
      case a: Attribute =>
        targetAttrs.getOrElse(a.exprId, a)
      case UnresolvedExtractValue(child, extraction) =>
        ExtractValue(child, extraction, resolver)
    })

    resolvedSource == target
  }
}
