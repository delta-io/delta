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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.DeltaParquetFileFormat._
import org.apache.spark.sql.delta.commands.DeletionVectorUtils.deletionVectorsReadable
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.FileFormat.METADATA_NAME
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

/**
 * Plan transformer to inject a filter that removes the rows marked as deleted according to
 * deletion vectors. For tables with no deletion vectors, this transformation has no effect.
 *
 * It modifies for plan for tables with deletion vectors as follows:
 * Before rule: <Parent Node> -> Delta Scan (key, value).
 *   - Here we are reading `key`, `value`` columns from the Delta table
 * After rule:
 *   <Parent Node> ->
 *     Project(key, value) ->
 *       Filter (__skip_row == 0) ->
 *         Delta Scan (key, value, __skip_row)
 *   - Here we insert a new column `__skip_row` in Delta scan. This value is populated by the
 *     Parquet reader using the DV corresponding to the Parquet file read
 *     (See [[DeltaParquetFileFormat]]) and it contains 0 if we want to keep the row.
 *   - Filter created filters out rows with __skip_row equals to 0
 *   - And at the end we have a Project to keep the plan node output same as before the rule is
 *     applied.
 */
trait PreprocessTableWithDVs extends SubqueryTransformerHelper {
  def preprocessTablesWithDVs(plan: LogicalPlan): LogicalPlan = {
    transformWithSubqueries(plan) {
      case ScanWithDeletionVectors(dvScan) => dvScan
    }
  }
}

object ScanWithDeletionVectors {
  def unapply(a: LogicalRelation): Option[LogicalPlan] = a match {
    case scan @ LogicalRelation(
            relation @ HadoopFsRelation(
            index: TahoeFileIndex, _, _, _, format: DeltaParquetFileFormat, _), _, _, _) =>
      dvEnabledScanFor(scan, relation, format, index)
    case _ => None
  }

  def dvEnabledScanFor(
      scan: LogicalRelation,
      hadoopRelation: HadoopFsRelation,
      fileFormat: DeltaParquetFileFormat,
      index: TahoeFileIndex): Option[LogicalPlan] = {
    // If the table has no DVs enabled, no change needed
    if (!deletionVectorsReadable(index.protocol, index.metadata)) return None

    require(!index.isInstanceOf[TahoeLogFileIndex],
      "Cannot work with a non-pinned table snapshot of the TahoeFileIndex")

    // See if the relation is already modified to include DV reads as part of
    // a previous invocation of this rule on this table
    if (fileFormat.hasTablePath) return None

    // See if any files actually have a DV.
    val filesWithDVs = index
      .matchingFiles(partitionFilters = Seq(TrueLiteral), dataFilters = Seq(TrueLiteral))
      .filter(_.deletionVector != null)
    if (filesWithDVs.isEmpty) return None

    // Get the list of columns in the output of the `LogicalRelation` we are
    // trying to modify. At the end of the plan, we need to return a
    // `LogicalRelation` that has the same output as this `LogicalRelation`
    val planOutput = scan.output

    val spark = SparkSession.getActiveSession.get
    val newScan = createScanWithSkipRowColumn(spark, scan, fileFormat, index, hadoopRelation)

    // On top of the scan add a filter that filters out the rows which have
    // skip row column value non-zero
    val rowIndexFilter = createRowIndexFilterNode(newScan)

    // Now add a project on top of the row index filter node to
    // remove the skip row column
    Some(Project(planOutput, rowIndexFilter))
  }

  /**
   * Helper function that adds row_index column to _metadata if missing.
   */
  private def addRowIndexIfMissing(attribute: AttributeReference): AttributeReference = {
    require(attribute.name == METADATA_NAME)

    val dataType = attribute.dataType.asInstanceOf[StructType]
    if (dataType.fieldNames.contains(ParquetFileFormat.ROW_INDEX)) return attribute

    val newDatatype = dataType.add(ParquetFileFormat.ROW_INDEX_FIELD)
    attribute.copy(
      dataType = newDatatype)(exprId = attribute.exprId, qualifier = attribute.qualifier)
  }

  /**
   * Helper method that creates a new `LogicalRelation` for existing scan that outputs
   * an extra column which indicates whether the row needs to be skipped or not.
   */
  private def createScanWithSkipRowColumn(
      spark: SparkSession,
      inputScan: LogicalRelation,
      fileFormat: DeltaParquetFileFormat,
      tahoeFileIndex: TahoeFileIndex,
      hadoopFsRelation: HadoopFsRelation): LogicalRelation = {
    val useMetadataRowIndex =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)

    // Create a new `LogicalRelation` that has modified `DeltaFileFormat` and output with an extra
    // column to indicate whether to skip the row or not

    // Add a column for SKIP_ROW to the base output. Value of 0 means the row needs be kept, any
    // other values mean the row needs be skipped.
    val skipRowField = IS_ROW_DELETED_STRUCT_FIELD

    val scanOutputWithMetadata = if (useMetadataRowIndex) {
      // When predicate pushdown is enabled, make sure the output contains metadata.row_index.
      if (inputScan.output.map(_.name).contains(METADATA_NAME)) {
        // If the scan already contains a metadata column without a row_index, add it.
        inputScan.output.collect {
          case a: AttributeReference if a.name == METADATA_NAME => addRowIndexIfMissing(a)
          case o => o
        }
      } else {
        inputScan.output :+ fileFormat.createFileMetadataCol()
      }
    } else {
      inputScan.output
    }

    val newScanOutput =
      scanOutputWithMetadata :+ AttributeReference(skipRowField.name, skipRowField.dataType)()

    // Data schema and scan schema could be different. The scan schema may contain additional
    // columns such as `_metadata.file_path` (metadata columns) which are populated in Spark scan
    // operator after the data is read from the underlying file reader.
    val newDataSchema = hadoopFsRelation.dataSchema.add(skipRowField)

    val newFileFormat = fileFormat.copyWithDVInfo(
      tablePath = tahoeFileIndex.path.toString,
      optimizationsEnabled = useMetadataRowIndex)

    val newRelation = hadoopFsRelation.copy(
      fileFormat = newFileFormat,
      dataSchema = newDataSchema)(hadoopFsRelation.sparkSession)

    // Create a new scan LogicalRelation
    inputScan.copy(relation = newRelation, output = newScanOutput)
  }

  private def createRowIndexFilterNode(newScan: LogicalRelation): Filter = {
    val skipRowColumnRefs = newScan.output.filter(_.name == IS_ROW_DELETED_COLUMN_NAME)
    require(skipRowColumnRefs.size == 1,
      s"Expected only one column with name=$IS_ROW_DELETED_COLUMN_NAME")
    val skipRowColumnRef = skipRowColumnRefs.head

    Filter(EqualTo(skipRowColumnRef, Literal(RowIndexFilter.KEEP_ROW_VALUE)), newScan)
  }
}
