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
package io.delta.spark.internal.v2.read.cdc;

import java.io.Serializable;
import org.apache.spark.sql.delta.commands.cdc.CDCReader;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Holds the augmented read schema and column indices for the three CDC columns ({@code
 * _change_type}, {@code _commit_version}, {@code _commit_timestamp}).
 *
 * <p>The augmented schema is passed to the Parquet reader so that missing CDC columns are filled
 * with null via schema evolution. {@link CDCReadFunction} then replaces those nulls with per-file
 * constants using the pre-computed indices.
 */
public class CDCSchemaContext implements Serializable {

  // Fixed ID to ensure serialization compatibility across compiled versions of this class.
  private static final long serialVersionUID = 1L;

  public static final String CDC_TYPE_COLUMN = CDCReader.CDC_TYPE_COLUMN_NAME();
  public static final String CDC_COMMIT_VERSION = CDCReader.CDC_COMMIT_VERSION();
  public static final String CDC_COMMIT_TIMESTAMP = CDCReader.CDC_COMMIT_TIMESTAMP();

  private static final StructField[] CDC_FIELDS =
      new StructField[] {
        DataTypes.createStructField(CDC_TYPE_COLUMN, DataTypes.StringType, true),
        DataTypes.createStructField(CDC_COMMIT_VERSION, DataTypes.LongType, true),
        DataTypes.createStructField(CDC_COMMIT_TIMESTAMP, DataTypes.TimestampType, true)
      };

  /** Read data schema augmented with CDC columns (for Parquet reader). */
  private final StructType readDataSchemaWithCDC;

  /** Index of _change_type in the internal row [readDataSchema, CDC, partition]. */
  private final int changeTypeInternalIndex;

  /** Index of _commit_version in the internal row. */
  private final int commitVersionInternalIndex;

  /** Index of _commit_timestamp in the internal row. */
  private final int commitTimestampInternalIndex;

  /**
   * Mapping from output ordinal to internal batch ordinal. Output order is [tableSchema, CDC].
   * Internal batch order is [readDataSchema, CDC(3), partitionSchema]. For each output column, this
   * array gives the internal batch index to read from. CDC output ordinals map to -1 (handled by
   * constants).
   */
  private final int[] outputToInternalMapping;

  /** Number of table columns (non-CDC) in the output. */
  private final int tableColCount;

  /**
   * @param readDataSchema the data schema without CDC/partition columns (from column pruning)
   * @param partitionSchema the partition schema
   * @param tableSchema the full table schema in original column order (data + partition
   *     interleaved)
   */
  public CDCSchemaContext(
      StructType readDataSchema, StructType partitionSchema, StructType tableSchema) {
    this.readDataSchemaWithCDC = appendCDCColumns(readDataSchema);
    int dataColCount = readDataSchema.fields().length;
    this.changeTypeInternalIndex = dataColCount;
    this.commitVersionInternalIndex = dataColCount + 1;
    this.commitTimestampInternalIndex = dataColCount + 2;
    this.tableColCount = tableSchema.fields().length;

    // Build mapping: output ordinal (table.schema() order) → internal batch ordinal.
    // Internal batch layout: [readDataSchema(0..d-1), CDC(d, d+1, d+2), partition(d+3..d+3+p-1)]
    // Output layout: [tableSchema columns in original order, CDC(3)]
    //
    // For each tableSchema column, find it in either readDataSchema or partitionSchema to get
    // its internal index.
    java.util.Map<String, Integer> dataColIndex = new java.util.HashMap<>();
    for (int i = 0; i < readDataSchema.fields().length; i++) {
      dataColIndex.put(readDataSchema.fields()[i].name(), i);
    }
    java.util.Map<String, Integer> partColIndex = new java.util.HashMap<>();
    for (int i = 0; i < partitionSchema.fields().length; i++) {
      partColIndex.put(partitionSchema.fields()[i].name(), dataColCount + 3 + i);
    }

    int totalOutputCols = tableColCount + CDC_FIELDS.length;
    this.outputToInternalMapping = new int[totalOutputCols];
    for (int i = 0; i < tableColCount; i++) {
      String colName = tableSchema.fields()[i].name();
      if (dataColIndex.containsKey(colName)) {
        outputToInternalMapping[i] = dataColIndex.get(colName);
      } else if (partColIndex.containsKey(colName)) {
        outputToInternalMapping[i] = partColIndex.get(colName);
      } else {
        throw new IllegalStateException(
            "Column '" + colName + "' not found in readDataSchema or partitionSchema");
      }
    }
    // CDC output ordinals → -1 (handled by constants)
    for (int i = 0; i < CDC_FIELDS.length; i++) {
      outputToInternalMapping[tableColCount + i] = -1;
    }
  }

  /** Returns the 3 CDC StructFields. */
  public static StructField[] cdcFields() {
    return CDC_FIELDS.clone();
  }

  /** Returns true if the given field name is a CDC column. */
  public static boolean isCDCColumn(String name) {
    return CDC_TYPE_COLUMN.equals(name)
        || CDC_COMMIT_VERSION.equals(name)
        || CDC_COMMIT_TIMESTAMP.equals(name);
  }

  /** Appends CDC columns to a schema. */
  private static StructType appendCDCColumns(StructType schema) {
    StructType result = schema;
    for (StructField field : CDC_FIELDS) {
      result = result.add(field);
    }
    return result;
  }

  public StructType getReadDataSchemaWithCDC() {
    return readDataSchemaWithCDC;
  }

  public int getChangeTypeInternalIndex() {
    return changeTypeInternalIndex;
  }

  public int getCommitVersionInternalIndex() {
    return commitVersionInternalIndex;
  }

  public int getCommitTimestampInternalIndex() {
    return commitTimestampInternalIndex;
  }

  /** Returns the mapping from output ordinal to internal batch ordinal. */
  public int[] getOutputToInternalMapping() {
    return outputToInternalMapping.clone();
  }

  /** Returns the number of table columns (non-CDC) in the output. */
  public int getTableColCount() {
    return tableColCount;
  }
}
