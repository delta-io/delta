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

import java.net.URI

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.RowIndexFilterType
import org.apache.spark.sql.delta.DeltaParquetFileFormat._
import org.apache.spark.sql.delta.actions.{DeletionVectorDescriptor, Metadata, Protocol}
import org.apache.spark.sql.delta.deletionvectors.{DropMarkedRowsFilter, KeepAllRowsFilter, KeepMarkedRowsFilter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ByteType, LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
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
    isSplittable: Boolean = true,
    disablePushDowns: Boolean = false,
    tablePath: Option[String] = None,
    broadcastDvMap: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]] = None,
    broadcastHadoopConf: Option[Broadcast[SerializableConfiguration]] = None)
  extends ParquetFileFormat {
  // Validate either we have all arguments for DV enabled read or none of them.
  if (hasDeletionVectorMap) {
    require(tablePath.isDefined && !isSplittable && disablePushDowns,
      "Wrong arguments for Delta table scan with deletion vectors")
  }

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

  def prepareSchema(inputSchema: StructType): StructType = {
    DeltaColumnMapping.createPhysicalSchema(inputSchema, referenceSchema, columnMappingMode)
  }

  override def isSplitable(
    sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = isSplittable

  def hasDeletionVectorMap(): Boolean = broadcastDvMap.isDefined && broadcastHadoopConf.isDefined

  /**
   * We sometimes need to replace FileFormat within LogicalPlans, so we have to override
   * `equals` to ensure file format changes are captured
   */
  override def equals(other: Any): Boolean = {
    other match {
      case ff: DeltaParquetFileFormat =>
        ff.columnMappingMode == columnMappingMode &&
        ff.referenceSchema == referenceSchema &&
        ff.isSplittable == isSplittable &&
        ff.disablePushDowns == disablePushDowns
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
    val pushdownFilters = if (disablePushDowns) Seq.empty else filters

    val parquetDataReader: PartitionedFile => Iterator[InternalRow] =
      super.buildReaderWithPartitionValues(
        sparkSession,
        prepareSchema(dataSchema),
        prepareSchema(partitionSchema),
        prepareSchema(requiredSchema),
        pushdownFilters,
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
    val rowIndexColumn = findColumn(ROW_INDEX_COLUMN_NAME)

    if (isRowDeletedColumn.isEmpty && rowIndexColumn.isEmpty) {
      return parquetDataReader // no additional metadata is needed.
    } else {
      // verify the file splitting and filter pushdown are disabled. The new additional
      // metadata columns cannot be generated with file splitting and filter pushdowns
      require(!isSplittable, "Cannot generate row index related metadata with file splitting")
      require(disablePushDowns, "Cannot generate row index related metadata with filter pushdown")
    }

    if (hasDeletionVectorMap && isRowDeletedColumn.isEmpty) {
        throw new IllegalArgumentException(
          s"Expected a column $IS_ROW_DELETED_COLUMN_NAME in the schema")
    }

    val useOffHeapBuffers = sparkSession.sessionState.conf.offHeapColumnVectorEnabled
    (partitionedFile: PartitionedFile) => {
      val rowIteratorFromParquet = parquetDataReader(partitionedFile)
      val iterToReturn =
        iteratorWithAdditionalMetadataColumns(
          partitionedFile,
          rowIteratorFromParquet,
          isRowDeletedColumn,
          useOffHeapBuffers = useOffHeapBuffers,
          rowIndexColumn = rowIndexColumn)
      iterToReturn.asInstanceOf[Iterator[InternalRow]]
    }
  }

  override def supportFieldName(name: String): Boolean = {
    if (columnMappingMode != NoMapping) true else super.supportFieldName(name)
  }

  def copyWithDVInfo(
      tablePath: String,
      broadcastDvMap: Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]],
      broadcastHadoopConf: Broadcast[SerializableConfiguration]): DeltaParquetFileFormat = {
    this.copy(
      isSplittable = false,
      disablePushDowns = true,
      tablePath = Some(tablePath),
      broadcastDvMap = Some(broadcastDvMap),
      broadcastHadoopConf = Some(broadcastHadoopConf))
  }

  /**
   * Modifies the data read from underlying Parquet reader by populating one or both of the
   * following metadata columns.
   *   - [[IS_ROW_DELETED_COLUMN_NAME]] - row deleted status from deletion vector corresponding
   *   to this file
   *   - [[ROW_INDEX_COLUMN_NAME]] - index of the row within the file.
   */
  private def iteratorWithAdditionalMetadataColumns(
      partitionedFile: PartitionedFile,
      iterator: Iterator[Object],
      isRowDeletedColumn: Option[ColumnMetadata],
      rowIndexColumn: Option[ColumnMetadata],
      useOffHeapBuffers: Boolean): Iterator[Object] = {
    val pathUri = new URI(partitionedFile.filePath)

    val rowIndexFilter = isRowDeletedColumn.map { col =>
      // Fetch the DV descriptor from the broadcast map and create a row index filter
      broadcastDvMap.get.value
        .get(pathUri)
        .map { case DeletionVectorDescriptorWithFilterType(dvDescriptor, filterType) =>
          filterType match {
            case i if i == RowIndexFilterType.IF_CONTAINED =>
              DropMarkedRowsFilter.createInstance(
                dvDescriptor,
                broadcastHadoopConf.get.value.value,
                tablePath.map(new Path(_)))
            case i if i == RowIndexFilterType.IF_NOT_CONTAINED =>
              KeepMarkedRowsFilter.createInstance(
                dvDescriptor,
                broadcastHadoopConf.get.value.value,
                tablePath.map(new Path(_)))
          }
        }
        .getOrElse(KeepAllRowsFilter)
    }

    val metadataColumns = Seq(isRowDeletedColumn, rowIndexColumn).filter(_.nonEmpty).map(_.get)

    // Unfortunately there is no way to verify the Parquet index is starting from 0.
    // We disable the splits, so the assumption is ParquetFileFormat respects that
    var rowIndex: Long = 0

    // Used only when non-column row batches are received from the Parquet reader
    val tempVector = new OnHeapColumnVector(1, ByteType)

    iterator.map { row =>
      row match {
        case batch: ColumnarBatch => // When vectorized Parquet reader is enabled
          val size = batch.numRows()
          // Create vectors for all needed metadata columns.
          // We can't use the one from Parquet reader as it set the
          // [[WritableColumnVector.isAllNulls]] to true and it can't be reset with using any
          // public APIs.
          trySafely(useOffHeapBuffers, size, metadataColumns) { writableVectors =>
            val indexVectorTuples = new ArrayBuffer[(Int, ColumnVector)]
            var index = 0
            isRowDeletedColumn.foreach { columnMetadata =>
              val isRowDeletedVector = writableVectors(index)
              rowIndexFilter.get
                .materializeIntoVector(rowIndex, rowIndex + size, isRowDeletedVector)
              indexVectorTuples += (columnMetadata.index -> isRowDeletedVector)
              index += 1
            }

            rowIndexColumn.foreach { columnMetadata =>
              val rowIndexVector = writableVectors(index)
              // populate the row index column value
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

        case rest: InternalRow => // When vectorized Parquet reader is disabled
          // Temporary vector variable used to get DV values from RowIndexFilter
          // Currently the RowIndexFilter only supports writing into a columnar vector
          // and doesn't have methods to get DV value for a specific row index.
          // TODO: This is not efficient, but it is ok given the default reader is vectorized
          // reader and this will be temporary until Delta upgrades to Spark with Parquet
          // reader that automatically generates the row index column.
          isRowDeletedColumn.foreach { columnMetadata =>
            rowIndexFilter.get.materializeIntoVector(rowIndex, rowIndex + 1, tempVector)
            rest.setLong(columnMetadata.index, tempVector.getByte(0))
          }

          rowIndexColumn.foreach(columnMetadata => rest.setLong(columnMetadata.index, rowIndex))
          rowIndex += 1
          rest
        case others =>
          throw new RuntimeException(
            s"Parquet reader returned an unknown row type: ${others.getClass.getName}")
      }
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
  val ROW_INDEX_STRUCT_FILED = StructField(ROW_INDEX_COLUMN_NAME, LongType)

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

  /** Helper class that encapsulate an [[RowIndexFilterType]]. */
  case class DeletionVectorDescriptorWithFilterType(
      descriptor: DeletionVectorDescriptor,
      filterType: RowIndexFilterType) {
  }
}
