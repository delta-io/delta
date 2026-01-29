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
package io.delta.spark.internal.v2.read.deletionvector;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.spark.sql.delta.DeltaParquetFileFormat;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Seq;

/**
 * Schema context for deletion vector processing in the V2 connector.
 *
 * <p>Encapsulates schema with DV column and pre-computed indices needed for DV filtering.
 */
public class DvSchemaContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private final StructType schemaWithDvColumn;
  private final int dvColumnIndex;
  private final int inputColumnCount;
  private final StructType outputSchema;
  private final Seq<Object> outputColumnOrdinals;

  /**
   * Create a DV schema context for encapsulating schema info and indices needed for DV filtering.
   *
   * @param readDataSchema original data schema without DV column
   * @param partitionSchema partition columns schema
   * @throws IllegalArgumentException if readDataSchema already contains the DV column
   */
  public DvSchemaContext(StructType readDataSchema, StructType partitionSchema) {
    // Validate that readDataSchema doesn't already contain the DV column to ensure the DV column
    // is added only once. While Delta uses the "__delta_internal_" prefix as a naming convention
    // for internal columns (listed in DeltaColumnMapping.DELTA_INTERNAL_COLUMNS), there's no
    // enforced schema validation that prevents users from creating such columns. This check
    // provides a safety guard in the V2 connector.
    String dvColumnName = DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME();
    if (Arrays.asList(readDataSchema.fieldNames()).contains(dvColumnName)) {
      throw new IllegalArgumentException(
          "readDataSchema already contains the deletion vector column: " + dvColumnName);
    }
    this.schemaWithDvColumn =
        readDataSchema.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD());
    this.dvColumnIndex =
        schemaWithDvColumn.fieldIndex(DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME());
    this.inputColumnCount = schemaWithDvColumn.fields().length + partitionSchema.fields().length;
    this.outputSchema = readDataSchema.merge(partitionSchema, /* handleDuplicateColumns= */ false);
    // Pre-compute output column ordinals: all indices except dvColumnIndex.
    int[] ordinals = new int[inputColumnCount - 1];
    int idx = 0;
    for (int i = 0; i < inputColumnCount; i++) {
      if (i != dvColumnIndex) {
        ordinals[idx++] = i;
      }
    }
    this.outputColumnOrdinals = scala.Predef.wrapIntArray(ordinals).toSeq();
  }

  /** Returns schema with the __delta_internal_is_row_deleted column added. */
  public StructType getSchemaWithDvColumn() {
    return schemaWithDvColumn;
  }

  public int getDvColumnIndex() {
    return dvColumnIndex;
  }

  public int getInputColumnCount() {
    return inputColumnCount;
  }

  public StructType getOutputSchema() {
    return outputSchema;
  }

  /** Returns pre-computed output column ordinals for ProjectingInternalRow. */
  public Seq<Object> getOutputColumnOrdinals() {
    return outputColumnOrdinals;
  }
}
