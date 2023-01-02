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

import org.apache.spark.sql.delta.DeltaParquetFileFormat.newVector
import org.apache.spark.sql.delta.DeltaParquetFileFormat.trySafely
import org.apache.spark.sql.delta.actions.{DeletionVectorDescriptor, Metadata}
import org.apache.spark.sql.delta.deletionvectors.DeletedRowsMarkingFilter
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
import org.apache.spark.sql.types.{ByteType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.SerializableConfiguration

/** A thin wrapper over the Parquet file format to support columns names without restrictions. */
class DeltaParquetFileFormat(
    metadata: Metadata,
    val isSplittable: Boolean = true,
    val disablePushDowns: Boolean = false,
    val tablePath: Option[String] = None,
    val broadcastDvMap: Option[Broadcast[Map[String, DeletionVectorDescriptor]]] = None,
    val broadcastHadoopConf: Option[Broadcast[SerializableConfiguration]] = None)
  extends ParquetFileFormat {

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

    if (!hasDeletionVectorMap()) {
      return parquetDataReader
    }

    val skipRowColumnTypeIdx = requiredSchema.fields.zipWithIndex
      .find(_._1.name == DeltaParquetFileFormat.SKIP_ROW_NAME)

    if (skipRowColumnTypeIdx.isEmpty) {
      throw new IllegalArgumentException("Expected a column for skipping row in reader output")
    }

    val useOffHeapBuffers = sparkSession.sessionState.conf.offHeapColumnVectorEnabled

    (partitionedFile: PartitionedFile) => {
      val rowIteratorFromParquet = parquetDataReader(partitionedFile)
      val iterToReturn =
        iteratorWithRowIndexColumnAppended(
          partitionedFile,
          rowIteratorFromParquet,
          skipRowColumnTypeIdx.get._1,
          skipRowColumnTypeIdx.get._2,
          useOffHeapBuffers)
      iterToReturn.asInstanceOf[Iterator[InternalRow]]
    }
  }

  override def supportFieldName(name: String): Boolean = {
    if (columnMappingMode != NoMapping) true else super.supportFieldName(name)
  }

  def copyWithDVInfo(
      tablePath: String,
      broadcastDvMap: Broadcast[Map[String, DeletionVectorDescriptor]],
      broadcastHadoopConf: Broadcast[SerializableConfiguration]): DeltaParquetFileFormat = {
    new DeltaParquetFileFormat(
      metadata,
      isSplittable = false,
      disablePushDowns = true,
      tablePath = Some(tablePath),
      Some(broadcastDvMap),
      Some(broadcastHadoopConf))
  }

  private def iteratorWithRowIndexColumnAppended(
      partitionedFile: PartitionedFile,
      iterator: Iterator[Object],
      skipRowColumnType: StructField,
      skipRowColumnIndex: Int,
      useOffHeapBuffers: Boolean): Iterator[Object] = {
    val filePath = partitionedFile.filePath
    val absolutePath = new Path(filePath).toString

    // Fetch the DV descriptor from the broadcast map and create a row index filter
    val dvDescriptor = broadcastDvMap.get.value.get(absolutePath)
    val rowIndexFilter = DeletedRowsMarkingFilter.createInstance(
      dvDescriptor.getOrElse(DeletionVectorDescriptor.EMPTY),
      broadcastHadoopConf.get.value.value,
      tablePath.map(new Path(_))
    )

    // Unfortunately there is no way to verify the Parquet index is starting from 0.
    // We disable the splits, so the assumption is ParquetFileFormat respects that
    var rowIndex: Long = 0

    val newIter = iterator.map { row =>
      val newRow = row match {
        case batch: ColumnarBatch =>
          val size = batch.numRows()
          // Create a new vector for the skip row column. We can't use the one from Parquet reader
          // as it set the [[WritableColumnVector.isAllNulls]] to true and it can't be reset
          // with using any public APIs. In this new vector, fill the values using the row
          // index filter and replace the vector corresponding to skip row column in ColumnBatch
          val newBatch = trySafely(newVector(useOffHeapBuffers, size, skipRowColumnType)) {
            writableVector =>
              rowIndexFilter.materializeIntoVector(rowIndex, rowIndex + size, writableVector)
              rowIndex += size

              val vectors = ArrayBuffer[ColumnVector]()
              for (i <- 0 until batch.numCols()) {
                if (i == skipRowColumnIndex) {
                  vectors += writableVector
                  // Make sure to close the existing vector allocated in the Parquet
                  batch.column(i).close()
                } else {
                  vectors += batch.column(i)
                }
              }
              new ColumnarBatch(vectors.toArray, size)
          }
          newBatch

        case rest: InternalRow =>
          // Temporary vector variable used to get DV values from RowIndexFilter
          // Currently the RowIndexFilter only supports writing into a columnar vector
          // and doesn't have methods to get DV value for a specific row index.
          // TODO: This is not efficient, but it is ok given the default reader is vectorized
          // reader and this will be temporary until Delta upgrades to Spark with Parquet
          // reader that automatically generates the row index column.
          trySafely(new OnHeapColumnVector(1, ByteType)) { tempVector =>
            rowIndexFilter.materializeIntoVector(rowIndex, rowIndex + 1, tempVector)
            rest.setLong(skipRowColumnIndex, tempVector.getByte(0))
            rowIndex += 1
            rest
          }
        case others =>
          throw new RuntimeException(
            s"Parquet reader returned an unknown row type: ${others.getClass.getName}")
      }
      newRow
    }
    newIter
  }
}

object DeltaParquetFileFormat {
  /**
   * Column name used to identify whether the row read from the parquet file is marked
   * as deleted according to the Delta table deletion vectors
   */
  val SKIP_ROW_NAME = "__delta_internal_skip_row__"
  val SKIP_ROW_STRUCT_FIELD = StructField(SKIP_ROW_NAME, ByteType)

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
  def trySafely[R <: AutoCloseable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try {
      f.apply(resource)
    } catch {
      case NonFatal(e) =>
        resource.close()
        throw e
    }
  }
}
