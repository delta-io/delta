/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.read.metadata;

import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Test helpers for constructing file-source {@code _metadata} column schemas. */
public class MetadataColumnTestUtils {

  private MetadataColumnTestUtils() {}

  /**
   * Returns the top-level {@code _metadata} {@link StructField} carrying the file-source
   * metadata-column marker, built via the production {@code MetadataColumnsHelper.asStruct} path
   * (not a hand-rolled marker), so tests cannot drift from how prod stamps the column. {@code
   * metadataStruct} is the pruned struct of requested subfields.
   */
  public static StructField metadataColumnStructField(StructType metadataStruct) {
    MetadataColumn column =
        new MetadataColumn() {
          @Override
          public String name() {
            return FileFormat$.MODULE$.METADATA_NAME();
          }

          @Override
          public DataType dataType() {
            return metadataStruct;
          }

          @Override
          public boolean isNullable() {
            return false;
          }
        };
    return new DataSourceV2Implicits.MetadataColumnsHelper(new MetadataColumn[] {column})
        .asStruct()
        .fields()[0];
  }

  /**
   * Like {@link #metadataColumnStructField(StructType)} but with the struct's physical name changed
   * to {@code physicalName} (e.g. {@code __metadata}) while keeping the logical name {@code
   * _metadata} in its metadata marker. This mirrors the collision rename the analyzer performs
   * (physical != logical), which is the path the DSv2 read fix must handle.
   */
  public static StructField renamedMetadataColumnStructField(
      String physicalName, StructType metadataStruct) {
    StructField field = metadataColumnStructField(metadataStruct);
    return new StructField(physicalName, field.dataType(), field.nullable(), field.metadata());
  }
}
