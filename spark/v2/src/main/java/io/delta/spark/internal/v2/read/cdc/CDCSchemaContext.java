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
import scala.collection.immutable.Seq;

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

  public static final int CHANGE_TYPE_IDX = 0;
  public static final int COMMIT_VERSION_IDX = 1;
  public static final int COMMIT_TIMESTAMP_IDX = 2;

  private static final StructField[] CDC_FIELDS =
      new StructField[] {
        DataTypes.createStructField(CDC_TYPE_COLUMN, DataTypes.StringType, /* nullable= */ true),
        DataTypes.createStructField(CDC_COMMIT_VERSION, DataTypes.LongType, /* nullable= */ true),
        DataTypes.createStructField(
            CDC_COMMIT_TIMESTAMP, DataTypes.TimestampType, /* nullable= */ true)
      };

  /** Read data schema augmented with CDC columns (for Parquet reader). */
  private final StructType readDataSchemaWithCDC;

  /** Output schema for non-CDC columns: {@code readDataSchema + partition}. */
  private final StructType dataAndPartitionSchema;

  /** Index of _change_type in the internal row [readDataSchema, CDC, partition]. */
  private final int changeTypeInternalIndex;

  /**
   * Maps each (data + partition) output position to its internal batch position. Output order is
   * {@code [readDataSchema, partition]} (CDC columns are appended separately). Internal batch order
   * is {@code [readDataSchema, CDC, partition]}, so partition entries skip the CDC slots.
   */
  private final Seq<Object> dataAndPartitionOrdinals;

  /**
   * @param readDataSchema the (possibly pruned) data schema requested by Spark, without CDC or
   *     partition columns
   * @param partitionSchema the partition schema
   */
  public CDCSchemaContext(StructType readDataSchema, StructType partitionSchema) {
    this.readDataSchemaWithCDC = appendCDCColumns(readDataSchema);
    int dataColCount = readDataSchema.fields().length;
    int partColCount = partitionSchema.fields().length;
    int cdcColCount = CDC_FIELDS.length;
    this.changeTypeInternalIndex = dataColCount;
    this.dataAndPartitionSchema =
        readDataSchema.merge(partitionSchema, /* handleDuplicateColumns= */ false);

    // Output: [readDataSchema, partition, CDC]
    // Internal (from Parquet): [readDataSchema, CDC, partition]
    int[] ordinals = new int[dataColCount + partColCount];
    for (int i = 0; i < dataColCount; i++) {
      ordinals[i] = i;
    }
    for (int i = 0; i < partColCount; i++) {
      ordinals[dataColCount + i] = dataColCount + cdcColCount + i;
    }
    this.dataAndPartitionOrdinals = scala.Predef.wrapIntArray(ordinals).toList();
  }

  /** Returns the 3 CDC StructFields. */
  public static StructField[] cdcFields() {
    return CDC_FIELDS.clone();
  }

  /** Returns true if the given field name is a CDC column. */
  public static boolean isCDCColumn(String name) {
    return CDC_TYPE_COLUMN.equalsIgnoreCase(name)
        || CDC_COMMIT_VERSION.equalsIgnoreCase(name)
        || CDC_COMMIT_TIMESTAMP.equalsIgnoreCase(name);
  }

  /** Appends the three CDC columns to the given schema. */
  public static StructType appendCDCColumns(StructType schema) {
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

  /** Returns the data + partition output schema. */
  public StructType getDataAndPartitionSchema() {
    return dataAndPartitionSchema;
  }

  /**
   * Returns ordinals mapping each data + partition output position to its internal batch position.
   */
  public Seq<Object> getDataAndPartitionOrdinals() {
    return dataAndPartitionOrdinals;
  }
}
