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

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.RowIndexFilterType
import org.apache.spark.sql.delta.DeltaParquetFileFormat._
import org.apache.spark.sql.delta.actions.{DeletionVectorDescriptor, Metadata, Protocol}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils.deletionVectorsReadable
import org.apache.spark.sql.delta.deletionvectors.{DropMarkedRowsFilter, KeepAllRowsFilter, KeepMarkedRowsFilter}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.util.ContextUtil

import org.apache.spark.internal.{LoggingShims, MDC}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.FileSourceConstantMetadataStructField
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{ByteType, LongType, MetadataBuilder, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnarBatchRow, ColumnVector}
import org.apache.spark.util.SerializableConfiguration

/**
 * A thin wrapper over the Parquet file format to support
 *  - columns names without restrictions.
 *  - populated a column from the deletion vector of this file (if exists) to indicate
 *    whether the row is deleted or not according to the deletion vector. Consumers
 *    of this scan can use the column values to filter out the deleted rows.
 */
case class DeltaParquetFileFormat(
    protocol: Protocol,
    metadata: Metadata,
    nullableRowTrackingFields: Boolean = false,
    optimizationsEnabled: Boolean = true,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false)
  extends ParquetFileFormat
  with LoggingShims {
  // Validate either we have all arguments for DV enabled read or none of them.
  if (hasTablePath) {
    SparkSession.getActiveSession.map { session =>
      val useMetadataRowIndex =
        session.sessionState.conf.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)
      require(useMetadataRowIndex == optimizationsEnabled,
        "Wrong arguments for Delta table scan with deletion vectors")
    }
  }

  TypeWidening.assertTableReadable(protocol, metadata)

  val columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode
  val referenceSchema: StructType = metadata.schema

  if (columnMappingMode == IdMapping) {
    val requiredReadConf = SQLConf.PARQUET_FIELD_ID_READ_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredReadConf)),
      s"${requiredReadConf.key} must be enabled to support Delta id column mapping mode")
    val requiredWriteConf = SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredWriteConf)),
      s"${requiredWriteConf.key} must be enabled to support Delta id column mapping mode")
  }

  /**
   * prepareSchemaForRead must only be used for parquet read.
   * It removes "PARQUET_FIELD_ID_METADATA_KEY" for name mapping mode which address columns by
   * physical name instead of id.
   */
  def prepareSchemaForRead(inputSchema: StructType): StructType = {
    val schema = DeltaColumnMapping.createPhysicalSchema(
      inputSchema, referenceSchema, columnMappingMode)
    if (columnMappingMode == NameMapping) {
      SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
        field.copy(metadata = new MetadataBuilder()
          .withMetadata(field.metadata)
          .remove(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY)
          .remove(DeltaColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY)
          .build())
      }
    } else schema
  }

  /**
   * Prepares filters so that they can be pushed down into the Parquet reader.
   *
   * If column mapping is enabled, then logical column names in the filters will be replaced with
   * their corresponding physical column names. This is necessary as the Parquet files will use
   * physical column names, and the requested schema pushed down in the Parquet reader will also use
   * physical column names.
   */
  private def prepareFiltersForRead(filters: Seq[Filter]): Seq[Filter] = {
    if (!optimizationsEnabled) {
      Seq.empty
    } else if (columnMappingMode != NoMapping) {
      import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
      val physicalNameMap = DeltaColumnMapping.getLogicalNameToPhysicalNameMap(referenceSchema)
        .map { case (logicalName, physicalName) => (logicalName.quoted, physicalName.quoted) }
      filters.flatMap(translateFilterForColumnMapping(_, physicalNameMap))
    } else {
      filters
    }
  }

  override def isSplitable(
    sparkSession: SparkSession,
    options: Map[String, String],
    path: Path): Boolean = optimizationsEnabled

  def hasTablePath: Boolean = tablePath.isDefined

  /**
   * We sometimes need to replace FileFormat within LogicalPlans, so we have to override
   * `equals` to ensure file format changes are captured
   */
  override def equals(other: Any): Boolean = {
    other match {
      case ff: DeltaParquetFileFormat =>
        ff.columnMappingMode == columnMappingMode &&
        ff.referenceSchema == referenceSchema &&
        ff.optimizationsEnabled == optimizationsEnabled
      case _ => false
    }
  }

  override def hashCode(): Int = getClass.getCanonicalName.hashCode()

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val useMetadataRowIndexConf = DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX
    val useMetadataRowIndex = sparkSession.sessionState.conf.getConf(useMetadataRowIndexConf)

    val parquetDataReader: PartitionedFile => Iterator[InternalRow] =
      super.buildReaderWithPartitionValues(
        sparkSession,
        prepareSchemaForRead(dataSchema),
        prepareSchemaForRead(partitionSchema),
        prepareSchemaForRead(requiredSchema),
        prepareFiltersForRead(filters),
        options,
        hadoopConf)

    val schemaWithIndices = requiredSchema.fields.zipWithIndex
    def findColumn(name: String): Option[ColumnMetadata] = {
      val results = schemaWithIndices.filter(_._1.name == name)
      if (results.length > 1) {
        throw new IllegalArgumentException(
          s"There are more than one column with name=`$name` requested in the reader output")
      }
      results.headOption.map(e => ColumnMetadata(e._2, e._1))
    }

    val isRowDeletedColumn = findColumn(IS_ROW_DELETED_COLUMN_NAME)
    val rowIndexColumnName = if (useMetadataRowIndex) {
      ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME
    } else {
      ROW_INDEX_COLUMN_NAME
    }
    val rowIndexColumn = findColumn(rowIndexColumnName)

    // We don't have any additional columns to generate, just return the original reader as is.
    if (isRowDeletedColumn.isEmpty && rowIndexColumn.isEmpty) return parquetDataReader

    // We are using the row_index col generated by the parquet reader and there are no more
    // columns to generate.
    if (useMetadataRowIndex && isRowDeletedColumn.isEmpty) return parquetDataReader

    // Verify that either predicate pushdown with metadata column is enabled or optimizations
    // are disabled.
    require(useMetadataRowIndex || !optimizationsEnabled,
      "Cannot generate row index related metadata with file splitting or predicate pushdown")

    if (hasTablePath && isRowDeletedColumn.isEmpty) {
        throw new IllegalArgumentException(
          s"Expected a column $IS_ROW_DELETED_COLUMN_NAME in the schema")
    }

    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)

    val useOffHeapBuffers = sparkSession.sessionState.conf.offHeapColumnVectorEnabled
    (partitionedFile: PartitionedFile) => {
      val rowIteratorFromParquet = parquetDataReader(partitionedFile)
      try {
        val iterToReturn =
          iteratorWithAdditionalMetadataColumns(
            partitionedFile,
            rowIteratorFromParquet,
            isRowDeletedColumn,
            rowIndexColumn,
            useOffHeapBuffers,
            serializableHadoopConf,
            useMetadataRowIndex)
        iterToReturn.asInstanceOf[Iterator[InternalRow]]
      } catch {
        case NonFatal(e) =>
          // Close the iterator if it is a closeable resource. The `ParquetFileFormat` opens
          // the file and returns `RecordReaderIterator` (which implements `AutoCloseable` and
          // `Iterator`) instance as a `Iterator`.
          rowIteratorFromParquet match {
            case resource: AutoCloseable => closeQuietly(resource)
            case _ => // do nothing
          }
          throw e
      }
    }
  }

  override def supportFieldName(name: String): Boolean = {
    if (columnMappingMode != NoMapping) true else super.supportFieldName(name)
  }

  override def metadataSchemaFields: Seq[StructField] = {
    // TODO(SPARK-47731): Parquet reader in Spark has a bug where a file containing 2b+ rows
    // in a single rowgroup causes it to run out of the `Integer` range.
    // For Delta Parquet readers don't expose the row_index field as a metadata field when it is
    // not strictly required. We do expose it when Row Tracking or DVs are enabled.
    // In general, having 2b+ rows in a single rowgroup is not a common use case. When the issue is
    // hit an exception is thrown.
    (protocol, metadata) match {
      // We should not expose row tracking fields for CDC reads.
      case (p, m) if RowId.isEnabled(p, m) && !isCDCRead =>
        val extraFields = RowTracking.createMetadataStructFields(p, m, nullableRowTrackingFields)
        super.metadataSchemaFields ++ extraFields
      case (p, m) if deletionVectorsReadable(p, m) => super.metadataSchemaFields
      case _ => super.metadataSchemaFields.filter(_ != ParquetFileFormat.ROW_INDEX_FIELD)
    }
  }

  override def prepareWrite(
       sparkSession: SparkSession,
       job: Job,
       options: Map[String, String],
       dataSchema: StructType): OutputWriterFactory = {
    val factory = super.prepareWrite(sparkSession, job, options, dataSchema)
    val conf = ContextUtil.getConfiguration(job)
    // Always write timestamp as TIMESTAMP_MICROS for Iceberg compat based on Iceberg spec
    if (IcebergCompatV1.isEnabled(metadata) || IcebergCompatV2.isEnabled(metadata)) {
      conf.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
        SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS.toString)
    }
    if (IcebergCompatV2.isEnabled(metadata)) {
      // For Uniform with IcebergCompatV2, we need to write nested field IDs for list and map
      // types to the parquet schema. Spark currently does not support it so we hook in our
      // own write support class.
      ParquetOutputFormat.setWriteSupportClass(job, classOf[DeltaParquetWriteSupport])
    }
    factory
  }

  override def fileConstantMetadataExtractors: Map[String, PartitionedFile => Any] = {
    val extractBaseRowId: PartitionedFile => Any = { file =>
      file.otherConstantMetadataColumnValues.getOrElse(RowId.BASE_ROW_ID, {
        throw new IllegalStateException(
          s"Missing ${RowId.BASE_ROW_ID} value for file '${file.filePath}'")
      })
    }
    val extractDefaultRowCommitVersion: PartitionedFile => Any = { file =>
      file.otherConstantMetadataColumnValues
        .getOrElse(DefaultRowCommitVersion.METADATA_STRUCT_FIELD_NAME, {
          throw new IllegalStateException(
            s"Missing ${DefaultRowCommitVersion.METADATA_STRUCT_FIELD_NAME} value " +
              s"for file '${file.filePath}'")
        })
    }
    super.fileConstantMetadataExtractors
      .updated(RowId.BASE_ROW_ID, extractBaseRowId)
      .updated(DefaultRowCommitVersion.METADATA_STRUCT_FIELD_NAME, extractDefaultRowCommitVersion)
  }

  def copyWithDVInfo(
      tablePath: String,
      optimizationsEnabled: Boolean): DeltaParquetFileFormat = {
    // When predicate pushdown is enabled we allow both splits and predicate pushdown.
    this.copy(
      optimizationsEnabled = optimizationsEnabled,
      tablePath = Some(tablePath))
  }

  /**
   * Modifies the data read from underlying Parquet reader by populating one or both of the
   * following metadata columns.
   *   - [[IS_ROW_DELETED_COLUMN_NAME]] - row deleted status from deletion vector corresponding
   *   to this file
   *   - [[ROW_INDEX_COLUMN_NAME]] - index of the row within the file. Note, this column is only
   *     populated when we are not using _metadata.row_index column.
   */
  private def iteratorWithAdditionalMetadataColumns(
      partitionedFile: PartitionedFile,
      iterator: Iterator[Object],
      isRowDeletedColumnOpt: Option[ColumnMetadata],
      rowIndexColumnOpt: Option[ColumnMetadata],
      useOffHeapBuffers: Boolean,
      serializableHadoopConf: SerializableConfiguration,
      useMetadataRowIndex: Boolean): Iterator[Object] = {
    require(!useMetadataRowIndex || rowIndexColumnOpt.isDefined,
      "useMetadataRowIndex is enabled but rowIndexColumn is not defined.")

    val rowIndexFilterOpt = isRowDeletedColumnOpt.map { col =>
      // Fetch the DV descriptor from the broadcast map and create a row index filter
      val dvDescriptorOpt = partitionedFile.otherConstantMetadataColumnValues
        .get(FILE_ROW_INDEX_FILTER_ID_ENCODED)
      val filterTypeOpt = partitionedFile.otherConstantMetadataColumnValues
        .get(FILE_ROW_INDEX_FILTER_TYPE)
      if (dvDescriptorOpt.isDefined && filterTypeOpt.isDefined) {
        val rowIndexFilter = filterTypeOpt.get match {
          case RowIndexFilterType.IF_CONTAINED => DropMarkedRowsFilter
          case RowIndexFilterType.IF_NOT_CONTAINED => KeepMarkedRowsFilter
          case unexpectedFilterType => throw new IllegalStateException(
            s"Unexpected row index filter type: ${unexpectedFilterType}")
        }
        rowIndexFilter.createInstance(
          DeletionVectorDescriptor.deserializeFromBase64(dvDescriptorOpt.get.asInstanceOf[String]),
          serializableHadoopConf.value,
          tablePath.map(new Path(_)))
      } else if (dvDescriptorOpt.isDefined || filterTypeOpt.isDefined) {
        throw new IllegalStateException(
          s"Both ${FILE_ROW_INDEX_FILTER_ID_ENCODED} and ${FILE_ROW_INDEX_FILTER_TYPE} " +
            "should either both have values or no values at all.")
      } else {
        KeepAllRowsFilter
      }
    }

    // We only generate the row index column when predicate pushdown is not enabled.
    val rowIndexColumnToWriteOpt = if (useMetadataRowIndex) None else rowIndexColumnOpt
    val metadataColumnsToWrite =
      Seq(isRowDeletedColumnOpt, rowIndexColumnToWriteOpt).filter(_.nonEmpty).map(_.get)

    // When metadata.row_index is not used there is no way to verify the Parquet index is
    // starting from 0. We disable the splits, so the assumption is ParquetFileFormat respects
    // that.
    var rowIndex: Long = 0

    // Used only when non-column row batches are received from the Parquet reader
    val tempVector = new OnHeapColumnVector(1, ByteType)

    iterator.map { row =>
      row match {
        case batch: ColumnarBatch => // When vectorized Parquet reader is enabled.
          val size = batch.numRows()
          // Create vectors for all needed metadata columns.
          // We can't use the one from Parquet reader as it set the
          // [[WritableColumnVector.isAllNulls]] to true and it can't be reset with using any
          // public APIs.
          trySafely(useOffHeapBuffers, size, metadataColumnsToWrite) { writableVectors =>
            val indexVectorTuples = new ArrayBuffer[(Int, ColumnVector)]

            // When predicate pushdown is enabled we use _metadata.row_index. Therefore,
            // we only need to construct the isRowDeleted column.
            var index = 0
            isRowDeletedColumnOpt.foreach { columnMetadata =>
              val isRowDeletedVector = writableVectors(index)
              if (useMetadataRowIndex) {
                rowIndexFilterOpt.get.materializeIntoVectorWithRowIndex(
                  size, batch.column(rowIndexColumnOpt.get.index), isRowDeletedVector)
              } else {
                rowIndexFilterOpt.get
                  .materializeIntoVector(rowIndex, rowIndex + size, isRowDeletedVector)
              }
              indexVectorTuples += (columnMetadata.index -> isRowDeletedVector)
              index += 1
            }

            rowIndexColumnToWriteOpt.foreach { columnMetadata =>
              val rowIndexVector = writableVectors(index)
              // populate the row index column value.
              for (i <- 0 until size) {
                rowIndexVector.putLong(i, rowIndex + i)
              }

              indexVectorTuples += (columnMetadata.index -> rowIndexVector)
              index += 1
            }

            val newBatch = replaceVectors(batch, indexVectorTuples.toSeq: _*)
            rowIndex += size
            newBatch
          }

        case columnarRow: ColumnarBatchRow =>
          // When vectorized reader is enabled but returns immutable rows instead of
          // columnar batches [[ColumnarBatchRow]]. So we have to copy the row as a
          // mutable [[InternalRow]] and set the `row_index` and `is_row_deleted`
          // column values. This is not efficient. It should affect only the wide
          // tables. https://github.com/delta-io/delta/issues/2246
          val newRow = columnarRow.copy();
          isRowDeletedColumnOpt.foreach { columnMetadata =>
            val rowIndexForFiltering = if (useMetadataRowIndex) {
              columnarRow.getLong(rowIndexColumnOpt.get.index)
            } else {
              rowIndex
            }
            rowIndexFilterOpt.get.materializeSingleRowWithRowIndex(rowIndexForFiltering, tempVector)
            newRow.setByte(columnMetadata.index, tempVector.getByte(0))
          }

          rowIndexColumnToWriteOpt
            .foreach(columnMetadata => newRow.setLong(columnMetadata.index, rowIndex))
          rowIndex += 1

          newRow
        case rest: InternalRow => // When vectorized Parquet reader is disabled
          // Temporary vector variable used to get DV values from RowIndexFilter
          // Currently the RowIndexFilter only supports writing into a columnar vector
          // and doesn't have methods to get DV value for a specific row index.
          // TODO: This is not efficient, but it is ok given the default reader is vectorized
          isRowDeletedColumnOpt.foreach { columnMetadata =>
            val rowIndexForFiltering = if (useMetadataRowIndex) {
              rest.getLong(rowIndexColumnOpt.get.index)
            } else {
              rowIndex
            }
            rowIndexFilterOpt.get.materializeSingleRowWithRowIndex(rowIndexForFiltering, tempVector)
            rest.setByte(columnMetadata.index, tempVector.getByte(0))
          }

          rowIndexColumnToWriteOpt
            .foreach(columnMetadata => rest.setLong(columnMetadata.index, rowIndex))
          rowIndex += 1
          rest
        case others =>
          throw new RuntimeException(
            s"Parquet reader returned an unknown row type: ${others.getClass.getName}")
      }
    }
  }

  /**
   * Translates the filter to use physical column names instead of logical column names.
   * This is needed when the column mapping mode is set to `NameMapping` or `IdMapping`
   * to match the requested schema that's passed to the [[ParquetFileFormat]].
   */
  private def translateFilterForColumnMapping(
      filter: Filter,
      physicalNameMap: Map[String, String]): Option[Filter] = {
    object PhysicalAttribute {
      def unapply(attribute: String): Option[String] = {
        physicalNameMap.get(attribute)
      }
    }

    filter match {
      case EqualTo(PhysicalAttribute(physicalAttribute), value) =>
        Some(EqualTo(physicalAttribute, value))
      case EqualNullSafe(PhysicalAttribute(physicalAttribute), value) =>
        Some(EqualNullSafe(physicalAttribute, value))
      case GreaterThan(PhysicalAttribute(physicalAttribute), value) =>
        Some(GreaterThan(physicalAttribute, value))
      case GreaterThanOrEqual(PhysicalAttribute(physicalAttribute), value) =>
        Some(GreaterThanOrEqual(physicalAttribute, value))
      case LessThan(PhysicalAttribute(physicalAttribute), value) =>
        Some(LessThan(physicalAttribute, value))
      case LessThanOrEqual(PhysicalAttribute(physicalAttribute), value) =>
        Some(LessThanOrEqual(physicalAttribute, value))
      case In(PhysicalAttribute(physicalAttribute), values) =>
        Some(In(physicalAttribute, values))
      case IsNull(PhysicalAttribute(physicalAttribute)) =>
        Some(IsNull(physicalAttribute))
      case IsNotNull(PhysicalAttribute(physicalAttribute)) =>
        Some(IsNotNull(physicalAttribute))
      case And(left, right) =>
        val newLeft = translateFilterForColumnMapping(left, physicalNameMap)
        val newRight = translateFilterForColumnMapping(right, physicalNameMap)
        (newLeft, newRight) match {
          case (Some(l), Some(r)) => Some(And(l, r))
          case (Some(l), None) => Some(l)
          case (_, _) => newRight
        }
      case Or(left, right) =>
        val newLeft = translateFilterForColumnMapping(left, physicalNameMap)
        val newRight = translateFilterForColumnMapping(right, physicalNameMap)
        (newLeft, newRight) match {
          case (Some(l), Some(r)) => Some(Or(l, r))
          case (_, _) => None
        }
      case Not(child) =>
        translateFilterForColumnMapping(child, physicalNameMap).map(Not)
      case StringStartsWith(PhysicalAttribute(physicalAttribute), value) =>
        Some(StringStartsWith(physicalAttribute, value))
      case StringEndsWith(PhysicalAttribute(physicalAttribute), value) =>
        Some(StringEndsWith(physicalAttribute, value))
      case StringContains(PhysicalAttribute(physicalAttribute), value) =>
        Some(StringContains(physicalAttribute, value))
      case AlwaysTrue() => Some(AlwaysTrue())
      case AlwaysFalse() => Some(AlwaysFalse())
      case _ =>
        logError(log"Failed to translate filter ${MDC(DeltaLogKeys.FILTER, filter)}")
        None
    }
  }
}

object DeltaParquetFileFormat {
  /**
   * Column name used to identify whether the row read from the parquet file is marked
   * as deleted according to the Delta table deletion vectors
   */
  val IS_ROW_DELETED_COLUMN_NAME = "__delta_internal_is_row_deleted"
  val IS_ROW_DELETED_STRUCT_FIELD = StructField(IS_ROW_DELETED_COLUMN_NAME, ByteType)

  /** Row index for each column */
  val ROW_INDEX_COLUMN_NAME = "__delta_internal_row_index"
  val ROW_INDEX_STRUCT_FIELD = StructField(ROW_INDEX_COLUMN_NAME, LongType)

  /** The key to the encoded row index filter identifier value of the
   * [[PartitionedFile]]'s otherConstantMetadataColumnValues map. */
  val FILE_ROW_INDEX_FILTER_ID_ENCODED = "row_index_filter_id_encoded"

  /** The key to the row index filter type value of the
   * [[PartitionedFile]]'s otherConstantMetadataColumnValues map. */
  val FILE_ROW_INDEX_FILTER_TYPE = "row_index_filter_type"

  /** Utility method to create a new writable vector */
  private def newVector(
      useOffHeapBuffers: Boolean, size: Int, dataType: StructField): WritableColumnVector = {
    if (useOffHeapBuffers) {
      OffHeapColumnVector.allocateColumns(size, Seq(dataType).toArray)(0)
    } else {
      OnHeapColumnVector.allocateColumns(size, Seq(dataType).toArray)(0)
    }
  }

  /** Try the operation, if the operation fails release the created resource */
  private def trySafely[R <: WritableColumnVector, T](
      useOffHeapBuffers: Boolean,
      size: Int,
      columns: Seq[ColumnMetadata])(f: Seq[WritableColumnVector] => T): T = {
    val resources = new ArrayBuffer[WritableColumnVector](columns.size)
    try {
      columns.foreach(col => resources.append(newVector(useOffHeapBuffers, size, col.structField)))
      f(resources.toSeq)
    } catch {
      case NonFatal(e) =>
        resources.foreach(closeQuietly(_))
        throw e
    }
  }

  /** Utility method to quietly close an [[AutoCloseable]] */
  private def closeQuietly(closeable: AutoCloseable): Unit = {
    if (closeable != null) {
      try {
        closeable.close()
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  /**
   * Helper method to replace the vectors in given [[ColumnarBatch]].
   * New vectors and its index in the batch are given as tuples.
   */
  private def replaceVectors(
      batch: ColumnarBatch,
      indexVectorTuples: (Int, ColumnVector) *): ColumnarBatch = {
    val vectors = ArrayBuffer[ColumnVector]()
    for (i <- 0 until batch.numCols()) {
      var replaced: Boolean = false
      for (indexVectorTuple <- indexVectorTuples) {
        val index = indexVectorTuple._1
        val vector = indexVectorTuple._2
        if (indexVectorTuple._1 == i) {
          vectors += indexVectorTuple._2
          // Make sure to close the existing vector allocated in the Parquet
          batch.column(i).close()
          replaced = true
        }
      }
      if (!replaced) {
        vectors += batch.column(i)
      }
    }
    new ColumnarBatch(vectors.toArray, batch.numRows())
  }

  /** Helper class to encapsulate column info */
  case class ColumnMetadata(index: Int, structField: StructField)
}
