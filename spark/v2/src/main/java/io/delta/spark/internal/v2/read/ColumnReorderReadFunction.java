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
package io.delta.spark.internal.v2.read;

import io.delta.spark.internal.v2.utils.CloseableIterator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

/**
 * Reorders a Parquet reader's {@code readDataSchema ++ partitionSchema} output into the DDL field
 * order declared by {@link SparkScan#readSchema()}.
 */
public class ColumnReorderReadFunction
    extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
  private final boolean isVectorizedReader;
  private final StructType targetSchema;
  private final int[] reorderIndices;

  private ColumnReorderReadFunction(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      boolean isVectorizedReader,
      StructType targetSchema,
      int[] reorderIndices) {
    this.baseReadFunc = baseReadFunc;
    this.isVectorizedReader = isVectorizedReader;
    this.targetSchema = targetSchema;
    this.reorderIndices = reorderIndices;
  }

  @Override
  public Iterator<InternalRow> apply(PartitionedFile file) {
    return isVectorizedReader ? applyBatch(file) : applyRow(file);
  }

  private Iterator<InternalRow> applyRow(PartitionedFile file) {
    ProjectingInternalRow projection =
        ProjectingInternalRow.apply(
            targetSchema, scala.Predef.wrapIntArray(reorderIndices).toSeq());
    return CloseableIterator.wrap(baseReadFunc.apply(file))
        .mapCloseable(
            row -> {
              projection.project(row);
              return (InternalRow) projection;
            });
  }

  @SuppressWarnings("unchecked")
  private Iterator<InternalRow> applyBatch(PartitionedFile file) {
    Iterator<Object> baseIterator = (Iterator<Object>) (Iterator<?>) baseReadFunc.apply(file);
    return (Iterator<InternalRow>)
        (Iterator<?>)
            CloseableIterator.wrap(baseIterator)
                .mapCloseable(
                    item -> {
                      if (item instanceof ColumnarBatch) {
                        return reorderBatch((ColumnarBatch) item);
                      }
                      throw new IllegalStateException(
                          "Expected ColumnarBatch when vectorized reader is enabled, but got: "
                              + item.getClass());
                    });
  }

  private ColumnarBatch reorderBatch(ColumnarBatch batch) {
    ColumnVector[] reordered = new ColumnVector[reorderIndices.length];
    for (int i = 0; i < reorderIndices.length; i++) {
      reordered[i] = batch.column(reorderIndices[i]);
    }
    return new ColumnarBatch(reordered, batch.numRows());
  }

  /**
   * Factory method. Returns the {@code baseReadFunc} unchanged if the reorder is the identity (no
   * permutation needed - avoids a per-batch allocation).
   */
  public static Function1<PartitionedFile, Iterator<InternalRow>> wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      boolean isVectorizedReader,
      StructType dataSchema,
      StructType partitionSchema,
      StructType targetSchema) {
    StructType sourceSchema = readerOutputSchema(dataSchema, partitionSchema, targetSchema);
    int[] reorderIndices = computeReorderIndices(sourceSchema, targetSchema);
    if (isIdentityReorder(reorderIndices)) {
      return baseReadFunc;
    }
    return new ColumnReorderReadFunction(
        baseReadFunc, isVectorizedReader, targetSchema, reorderIndices);
  }

  /**
   * Rebuilds the reader's output field layout: {@code dataSchema} fields, then {@code
   * partitionSchema} fields, then any leftover fields from {@code targetSchema} (e.g. CDC) in the
   * order they appear in {@code targetSchema}.
   */
  private static StructType readerOutputSchema(
      StructType dataSchema, StructType partitionSchema, StructType targetSchema) {
    Set<String> dataPartitionNames = new HashSet<>(dataSchema.fields().length);
    for (StructField f : dataSchema.fields()) dataPartitionNames.add(f.name());
    for (StructField f : partitionSchema.fields()) dataPartitionNames.add(f.name());
    List<StructField> fields =
        new ArrayList<>(
            dataSchema.fields().length
                + partitionSchema.fields().length
                + targetSchema.fields().length);
    for (StructField f : dataSchema.fields()) fields.add(f);
    for (StructField f : partitionSchema.fields()) fields.add(f);
    for (StructField f : targetSchema.fields()) {
      if (!dataPartitionNames.contains(f.name())) {
        fields.add(f);
      }
    }
    return new StructType(fields.toArray(new StructField[0]));
  }

  /**
   * For each field in {@code targetSchema}, the offset of that field (by name) in {@code
   * sourceSchema}.
   */
  private static int[] computeReorderIndices(StructType sourceSchema, StructType targetSchema) {
    StructField[] sourceFields = sourceSchema.fields();
    StructField[] targetFields = targetSchema.fields();
    if (sourceFields.length != targetFields.length) {
      throw new IllegalStateException(
          "Source schema width "
              + sourceFields.length
              + " does not match target schema width "
              + targetFields.length
              + "; reader output has columns not in the DDL-ordered output schema. source="
              + sourceSchema
              + ", target="
              + targetSchema);
    }
    Map<String, Integer> sourceIndexByName = new HashMap<>();
    for (int i = 0; i < sourceFields.length; i++) {
      sourceIndexByName.put(sourceFields[i].name(), i);
    }
    int[] reorderIndices = new int[targetFields.length];
    for (int i = 0; i < targetFields.length; i++) {
      Integer idx = sourceIndexByName.get(targetFields[i].name());
      if (idx == null) {
        throw new IllegalStateException(
            "Cannot reorder columns: field '"
                + targetFields[i].name()
                + "' exists in the DDL-ordered target schema but is missing from the source schema "
                + sourceSchema);
      }
      reorderIndices[i] = idx;
    }
    return reorderIndices;
  }

  private static boolean isIdentityReorder(int[] reorderIndices) {
    for (int i = 0; i < reorderIndices.length; i++) {
      if (reorderIndices[i] != i) {
        return false;
      }
    }
    return true;
  }
}
