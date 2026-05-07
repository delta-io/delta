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
import java.util.OptionalInt;
import org.apache.spark.sql.delta.commands.cdc.CDCReader;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Records which CDC columns ({@code _change_type}, {@code _commit_version}, {@code
 * _commit_timestamp}) sit in {@code readDataSchema} and at what positions.
 */
public class CDCSchemaContext implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String CDC_TYPE_COLUMN = CDCReader.CDC_TYPE_COLUMN_NAME();
  public static final String CDC_COMMIT_VERSION = CDCReader.CDC_COMMIT_VERSION();
  public static final String CDC_COMMIT_TIMESTAMP = CDCReader.CDC_COMMIT_TIMESTAMP();

  private static final StructField[] CDC_FIELDS =
      new StructField[] {
        DataTypes.createStructField(CDC_TYPE_COLUMN, DataTypes.StringType, /* nullable= */ true),
        DataTypes.createStructField(CDC_COMMIT_VERSION, DataTypes.LongType, /* nullable= */ true),
        DataTypes.createStructField(
            CDC_COMMIT_TIMESTAMP, DataTypes.TimestampType, /* nullable= */ true)
      };

  /** Full schema of rows produced by the parquet reader: {@code readDataSchema ++ partition}. */
  private final StructType fullRowSchema;

  // -1 when the column is not in readDataSchema.
  private final int changeTypeIndex;
  private final int commitVersionIndex;
  private final int commitTimestampIndex;

  public CDCSchemaContext(StructType readDataSchema, StructType partitionSchema) {
    StructType merged = readDataSchema;
    for (StructField f : partitionSchema.fields()) {
      merged = merged.add(f);
    }
    this.fullRowSchema = merged;
    this.changeTypeIndex = findFieldIndex(readDataSchema, CDC_TYPE_COLUMN);
    this.commitVersionIndex = findFieldIndex(readDataSchema, CDC_COMMIT_VERSION);
    this.commitTimestampIndex = findFieldIndex(readDataSchema, CDC_COMMIT_TIMESTAMP);
  }

  /** Schema of rows the reader produces: {@code readDataSchema} followed by partition columns. */
  public StructType getFullRowSchema() {
    return fullRowSchema;
  }

  /** Index of {@code _change_type} in {@code readDataSchema}, or empty if not requested. */
  public OptionalInt changeTypeIndex() {
    return changeTypeIndex < 0 ? OptionalInt.empty() : OptionalInt.of(changeTypeIndex);
  }

  /** Index of {@code _commit_version} in {@code readDataSchema}, or empty if not requested. */
  public OptionalInt commitVersionIndex() {
    return commitVersionIndex < 0 ? OptionalInt.empty() : OptionalInt.of(commitVersionIndex);
  }

  /** Index of {@code _commit_timestamp} in {@code readDataSchema}, or empty if not requested. */
  public OptionalInt commitTimestampIndex() {
    return commitTimestampIndex < 0 ? OptionalInt.empty() : OptionalInt.of(commitTimestampIndex);
  }

  private static int findFieldIndex(StructType schema, String name) {
    StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].name().equalsIgnoreCase(name)) {
        return i;
      }
    }
    return -1;
  }

  /** Appends the three CDC columns to the given schema. */
  public static StructType appendCDCColumns(StructType schema) {
    StructType result = schema;
    for (StructField field : CDC_FIELDS) {
      result = result.add(field);
    }
    return result;
  }
}
