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
package io.delta.spark.internal.v2.read

import java.io.Serializable
import java.lang.{Integer => JInteger}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, HashSet => JHashSet}

import scala.runtime.AbstractFunction1

import io.delta.spark.internal.v2.utils.CloseableIterator

import org.apache.spark.sql.catalyst.{InternalRow, ProjectingInternalRow}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Reorders a Parquet reader's `readDataSchema ++ partitionSchema` output into the DDL field
 * order declared by [[DeltaV2Scan.readSchema]].
 */
@SerialVersionUID(1L)
class ColumnReorderReadFunction private (
    baseReadFuncInput: PartitionedFile => Iterator[InternalRow],
    isVectorizedReaderInput: Boolean,
    targetSchemaInput: StructType,
    reorderIndicesInput: Array[Int])
    extends AbstractFunction1[PartitionedFile, Iterator[InternalRow]]
    with Serializable {

  private[this] val baseReadFunc = baseReadFuncInput
  private[this] val isVectorizedReader = isVectorizedReaderInput
  private[this] val targetSchema = targetSchemaInput
  private[this] val reorderIndices = reorderIndicesInput

  override def apply(file: PartitionedFile): Iterator[InternalRow] = {
    if (isVectorizedReader) applyBatch(file) else applyRow(file)
  }

  private def applyRow(file: PartitionedFile): Iterator[InternalRow] = {
    val projection = ProjectingInternalRow(targetSchema, wrapIntArray(reorderIndices).toSeq)
    CloseableIterator.wrap(baseReadFunc(file)).mapCloseable[InternalRow] { row =>
      projection.project(row)
      projection
    }
  }

  private def applyBatch(file: PartitionedFile): Iterator[InternalRow] = {
    val baseIterator = baseReadFunc(file).asInstanceOf[Iterator[AnyRef]]
    CloseableIterator
      .wrap(baseIterator)
      .mapCloseable[ColumnarBatch] {
        case batch: ColumnarBatch => reorderBatch(batch)
        case item =>
          // format: off
          throw new IllegalStateException(
            "Expected ColumnarBatch when vectorized reader is enabled, but got: " + item.getClass
          )
          // format: on
      }
      .asInstanceOf[Iterator[InternalRow]]
  }

  private def reorderBatch(batch: ColumnarBatch): ColumnarBatch = {
    val reordered = new Array[ColumnVector](reorderIndices.length)
    var i = 0
    while (i < reorderIndices.length) {
      reordered(i) = batch.column(reorderIndices(i))
      i += 1
    }
    new ColumnarBatch(reordered, batch.numRows())
  }
}

object ColumnReorderReadFunction {

  /**
   * Factory method. Returns `baseReadFunc` unchanged if the reorder is the identity (no
   * permutation needed - avoids a per-batch allocation).
   */
  def wrap(
      baseReadFunc: PartitionedFile => Iterator[InternalRow],
      isVectorizedReader: Boolean,
      dataSchema: StructType,
      partitionSchema: StructType,
      targetSchema: StructType): PartitionedFile => Iterator[InternalRow] = {
    val sourceSchema = readerOutputSchema(dataSchema, partitionSchema, targetSchema)
    val reorderIndices = computeReorderIndices(sourceSchema, targetSchema)
    if (isIdentityReorder(reorderIndices)) baseReadFunc
    else {
      new ColumnReorderReadFunction(baseReadFunc, isVectorizedReader, targetSchema, reorderIndices)
    }
  }

  /**
   * Rebuilds the reader's output field layout: `dataSchema` fields, then `partitionSchema`
   * fields, then any leftover fields from `targetSchema` (e.g. CDC) in the order they appear in
   * `targetSchema`.
   */
  private def readerOutputSchema(
      dataSchema: StructType,
      partitionSchema: StructType,
      targetSchema: StructType): StructType = {
    val dataPartitionNames = new JHashSet[String](dataSchema.fields.length)
    val dataNameFields = dataSchema.fields
    var i = 0
    while (i < dataNameFields.length) {
      dataPartitionNames.add(dataNameFields(i).name)
      i += 1
    }
    val partitionNameFields = partitionSchema.fields
    i = 0
    while (i < partitionNameFields.length) {
      dataPartitionNames.add(partitionNameFields(i).name)
      i += 1
    }
    // format: off
    val fields = new JArrayList[StructField](
      dataSchema.fields.length + partitionSchema.fields.length + targetSchema.fields.length
    )
    // format: on
    val dataFields = dataSchema.fields
    i = 0
    while (i < dataFields.length) {
      fields.add(dataFields(i))
      i += 1
    }
    val partitionFields = partitionSchema.fields
    i = 0
    while (i < partitionFields.length) {
      fields.add(partitionFields(i))
      i += 1
    }
    val targetFields = targetSchema.fields
    i = 0
    while (i < targetFields.length) {
      val field = targetFields(i)
      if (!dataPartitionNames.contains(field.name)) {
        fields.add(field)
        ()
      }
      i += 1
    }
    new StructType(fields.toArray(new Array[StructField](0)))
  }

  /** For each field in `targetSchema`, the offset of that field (by name) in `sourceSchema`. */
  private def computeReorderIndices(
      sourceSchema: StructType,
      targetSchema: StructType): Array[Int] = {
    val sourceFields = sourceSchema.fields
    val targetFields = targetSchema.fields
    if (sourceFields.length != targetFields.length) {
      // format: off
      throw new IllegalStateException(
        "Source schema width " + sourceFields.length + " does not match target schema width " +
        targetFields.length +
        "; reader output has columns not in the DDL-ordered output schema. source=" +
        sourceSchema + ", target=" + targetSchema)
      // format: on
    }
    val sourceIndexByName = new JHashMap[String, JInteger]
    var i = 0
    while (i < sourceFields.length) {
      sourceIndexByName.put(sourceFields(i).name, JInteger.valueOf(i))
      i += 1
    }
    val reorderIndices = new Array[Int](targetFields.length)
    i = 0
    while (i < targetFields.length) {
      val idx = sourceIndexByName.get(targetFields(i).name)
      if (idx == null) {
        // format: off
        throw new IllegalStateException(
          "Cannot reorder columns: field '" + targetFields(i).name +
          "' exists in the DDL-ordered target schema but is missing from the source schema " +
          sourceSchema)
        // format: on
      }
      reorderIndices(i) = idx.intValue()
      i += 1
    }
    reorderIndices
  }

  private def isIdentityReorder(reorderIndices: Array[Int]): Boolean = {
    var i = 0
    while (i < reorderIndices.length) {
      if (reorderIndices(i) != i) return false
      i += 1
    }
    true
  }
}
