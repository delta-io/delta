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
package io.delta.spark.internal.v2.write;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link DeltaV2WriteBuilder}; the write-side counterpart of SparkScanBuilderTest.
 */
public class DeltaV2WriteBuilderTest extends DeltaV2TestBase {

  private static final StructType TABLE_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true)
          });

  @Test
  public void testBuild_returnsWrite(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE build_returns_write (id INT, name STRING) USING delta LOCATION '%s'",
            path));

    Write write = newBuilder(path, TABLE_SCHEMA, CaseInsensitiveStringMap.empty()).build();

    assertNotNull(write);
    assertTrue(write instanceof DeltaV2Write);
  }

  @Test
  public void testBuild_rejectsColumnMappedTable(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE build_rejects_cm (id INT, name STRING) USING delta LOCATION '%s' "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            path));

    DeltaV2WriteBuilder builder = newBuilder(path, TABLE_SCHEMA, CaseInsensitiveStringMap.empty());

    assertThrows(UnsupportedOperationException.class, builder::build);
  }

  @Test
  public void testBuild_rejectsIncompatibleSchema(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE build_rejects_schema (id INT, name STRING) USING delta LOCATION '%s'",
            path));

    // Write schema declares `id` as STRING while the table has INT -- an incompatible type with no
    // implicit conversion, which mergeSchemas rejects.
    StructType incompatible =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.StringType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    DeltaV2WriteBuilder builder = newBuilder(path, incompatible, CaseInsensitiveStringMap.empty());

    assertThrows(AnalysisException.class, builder::build);
  }

  private DeltaV2WriteBuilder newBuilder(
      String path, StructType writeSchema, CaseInsensitiveStringMap options) {
    Snapshot snapshot =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf())
            .loadLatestSnapshot();
    return new DeltaV2WriteBuilder(
        defaultEngine,
        path,
        spark.sessionState().newHadoopConf(),
        snapshot,
        TABLE_SCHEMA,
        WriteTestUtils.logicalWriteInfo(writeSchema, options));
  }
}
