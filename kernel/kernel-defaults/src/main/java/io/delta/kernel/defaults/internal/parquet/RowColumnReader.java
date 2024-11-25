/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.parquet;

import static io.delta.kernel.defaults.internal.parquet.ParquetSchemaUtils.findSubFieldType;
import static io.delta.kernel.defaults.internal.parquet.ParquetSchemaUtils.getParquetFieldToTypeMap;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultStructVector;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.*;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

/**
 * Row column readers for materializing the column values from Parquet files into Kernels {@link
 * ColumnVector}.
 */
class RowColumnReader extends GroupConverter implements ParquetColumnReaders.BaseColumnReader {
  private final StructType readSchema;
  private final Converter[] converters;
  // The delta may request columns that don't exists in Parquet
  // This map is to track the ordinal known to Parquet reader to the converter array ordinal.
  // If a column is missing, a dummy converter added to the `converters` array and which
  // generates all null vector at the end.
  private final Map<Integer, Integer> parquetOrdinalToConverterOrdinal;

  // Working state
  private boolean isCurrentValueNull = true;
  private int currentRowIndex;
  private boolean[] nullability;

  /**
   * Create converter for {@link StructType} column.
   *
   * @param initialBatchSize Estimate of initial row batch size. Used in memory allocations.
   * @param readSchema Schem of the columns to read from the file.
   * @param fileSchema Schema of the pruned columns from the file schema We have some necessary
   *     requirements here: (1) the fields in fileSchema are a subset of readSchema (parquet schema
   *     has been pruned). (2) the fields in fileSchema are in the same order as the corresponding
   *     fields in readSchema.
   */
  RowColumnReader(int initialBatchSize, StructType readSchema, GroupType fileSchema) {
    checkArgument(initialBatchSize > 0, "invalid initialBatchSize: %s", initialBatchSize);
    this.readSchema = requireNonNull(readSchema, "readSchema is not null");
    List<StructField> fields = readSchema.fields();
    this.converters = new Converter[fields.size()];
    this.parquetOrdinalToConverterOrdinal = new HashMap<>();

    // Initialize the working state
    this.nullability = ParquetColumnReaders.initNullabilityVector(initialBatchSize);

    int parquetOrdinal = 0;
    for (int i = 0; i < converters.length; i++) {
      final StructField field = fields.get(i);
      final DataType typeFromClient = field.getDataType();
      final Map<Integer, Type> parquetFieldIdToTypeMap = getParquetFieldToTypeMap(fileSchema);
      final Type typeFromFile =
          field.isDataColumn()
              ? findSubFieldType(fileSchema, field, parquetFieldIdToTypeMap)
              : null;
      if (typeFromFile == null) {
        if (StructField.METADATA_ROW_INDEX_COLUMN_NAME.equalsIgnoreCase(field.getName())
            && field.isMetadataColumn()) {
          checkArgument(
              field.getDataType() instanceof LongType,
              "row index metadata column must be type long");
          converters[i] = new ParquetColumnReaders.FileRowIndexColumnReader(initialBatchSize);
        } else {
          converters[i] = new ParquetColumnReaders.NonExistentColumnReader(typeFromClient);
        }
      } else {
        converters[i] =
            ParquetColumnReaders.createConverter(initialBatchSize, typeFromClient, typeFromFile);
        parquetOrdinalToConverterOrdinal.put(parquetOrdinal, i);
        parquetOrdinal++;
      }
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[parquetOrdinalToConverterOrdinal.get(fieldIndex)];
  }

  @Override
  public void start() {
    isCurrentValueNull = false;
  }

  @Override
  public void end() {}

  public ColumnarBatch getDataAsColumnarBatch(int batchSize) {
    ColumnVector[] memberVectors = collectMemberVectors(batchSize);
    ColumnarBatch batch = new DefaultColumnarBatch(batchSize, readSchema, memberVectors);
    resetWorkingState();
    return batch;
  }

  @Override
  public void finalizeCurrentRow(long currentRowIndex) {
    resizeIfNeeded();
    finalizeLastRowInConverters(currentRowIndex);
    nullability[this.currentRowIndex] = isCurrentValueNull;
    isCurrentValueNull = true;

    this.currentRowIndex++;
  }

  public ColumnVector getDataColumnVector(int batchSize) {
    ColumnVector[] memberVectors = collectMemberVectors(batchSize);
    ColumnVector vector =
        new DefaultStructVector(batchSize, readSchema, Optional.of(nullability), memberVectors);
    resetWorkingState();
    return vector;
  }

  @Override
  public void resizeIfNeeded() {
    if (nullability.length == currentRowIndex) {
      int newSize = nullability.length * 2;
      this.nullability = Arrays.copyOf(this.nullability, newSize);
      ParquetColumnReaders.setNullabilityToTrue(this.nullability, newSize / 2, newSize);
    }
  }

  @Override
  public void resetWorkingState() {
    this.currentRowIndex = 0;
    this.isCurrentValueNull = true;
    this.nullability = ParquetColumnReaders.initNullabilityVector(this.nullability.length);
  }

  private void finalizeLastRowInConverters(long prevRowIndex) {
    for (int i = 0; i < converters.length; i++) {
      ((ParquetColumnReaders.BaseColumnReader) converters[i]).finalizeCurrentRow(prevRowIndex);
    }
  }

  private ColumnVector[] collectMemberVectors(int batchSize) {
    final ColumnVector[] output = new ColumnVector[converters.length];

    for (int i = 0; i < converters.length; i++) {
      output[i] =
          ((ParquetColumnReaders.BaseColumnReader) converters[i]).getDataColumnVector(batchSize);
    }

    return output;
  }
}
