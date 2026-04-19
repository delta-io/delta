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
import java.util.HashMap;
import java.util.Map;
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
 * Wraps a Parquet reader function to reorder output columns from the underlying reader's {@code
 * readDataSchema ++ partitionSchema} layout into the target (DDL) order advertised by {@link
 * SparkScan#readSchema()}.
 *
 * <p>Needed because Spark's vectorized Parquet reader always appends partition columns at the end
 * of each {@link ColumnarBatch}, but the streaming plan binds output attributes to {@code
 * SparkTable.schema()} which preserves DDL order. Without this reorder, an ordinal-based downstream
 * consumer (e.g. whole-stage codegen) can read a type-mismatched {@link ColumnVector} and NPE (see
 * {@code OnHeapColumnVector.getLong}).
 */
public class ColumnReorderReadFunction
    extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
  private final boolean isVectorizedReader;
  private final StructType targetSchema;
  private final int[] permutation;

  private ColumnReorderReadFunction(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      boolean isVectorizedReader,
      StructType targetSchema,
      int[] permutation) {
    this.baseReadFunc = baseReadFunc;
    this.isVectorizedReader = isVectorizedReader;
    this.targetSchema = targetSchema;
    this.permutation = permutation;
  }

  @Override
  public Iterator<InternalRow> apply(PartitionedFile file) {
    return isVectorizedReader ? applyBatch(file) : applyRow(file);
  }

  private Iterator<InternalRow> applyRow(PartitionedFile file) {
    ProjectingInternalRow projection =
        ProjectingInternalRow.apply(targetSchema, scala.Predef.wrapIntArray(permutation).toSeq());
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
    ColumnVector[] reordered = new ColumnVector[permutation.length];
    for (int i = 0; i < permutation.length; i++) {
      reordered[i] = batch.column(permutation[i]);
    }
    return new ColumnarBatch(reordered, batch.numRows());
  }

  /**
   * Factory method. Returns the {@code baseReadFunc} unchanged if the permutation is the identity
   * (no reorder needed — saves a per-batch allocation).
   */
  public static Function1<PartitionedFile, Iterator<InternalRow>> wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      boolean isVectorizedReader,
      StructType sourceSchema,
      StructType targetSchema) {
    int[] permutation = computePermutation(sourceSchema, targetSchema);
    if (isIdentity(permutation)) {
      return baseReadFunc;
    }
    return new ColumnReorderReadFunction(
        baseReadFunc, isVectorizedReader, targetSchema, permutation);
  }

  /**
   * For each field in {@code targetSchema}, the index of that field (by name) in {@code
   * sourceSchema}.
   */
  private static int[] computePermutation(StructType sourceSchema, StructType targetSchema) {
    Map<String, Integer> sourceIndexByName = new HashMap<>();
    StructField[] sourceFields = sourceSchema.fields();
    for (int i = 0; i < sourceFields.length; i++) {
      sourceIndexByName.put(sourceFields[i].name(), i);
    }
    StructField[] targetFields = targetSchema.fields();
    int[] permutation = new int[targetFields.length];
    for (int i = 0; i < targetFields.length; i++) {
      Integer idx = sourceIndexByName.get(targetFields[i].name());
      if (idx == null) {
        throw new IllegalStateException(
            "Field " + targetFields[i].name() + " from target schema not found in source schema");
      }
      permutation[i] = idx;
    }
    return permutation;
  }

  private static boolean isIdentity(int[] permutation) {
    for (int i = 0; i < permutation.length; i++) {
      if (permutation[i] != i) {
        return false;
      }
    }
    return true;
  }
}
