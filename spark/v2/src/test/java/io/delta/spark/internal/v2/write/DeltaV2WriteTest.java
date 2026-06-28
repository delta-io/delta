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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.util.Map;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link DeltaV2Write}; the write-side counterpart of SparkScanTest. */
public class DeltaV2WriteTest extends DeltaV2TestBase {

  private static final StructType TABLE_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true)
          });

  /** Options that change write semantics and are rejected by the streaming append path. */
  private static final String[] UNSUPPORTED_STREAMING_OPTIONS = {
    "mergeSchema",
    "overwriteSchema",
    "replaceWhere",
    "replaceOn",
    "replaceUsing",
    "partitionOverwriteMode",
    "txnAppId",
    "txnVersion",
    "userMetadata"
  };

  @Test
  public void testToBatch_returnsDeltaV2BatchWrite(@TempDir File tempDir) {
    String path = createTable(tempDir, "write_to_batch");
    DeltaV2Write write = newWrite(path, CaseInsensitiveStringMap.empty());

    assertTrue(write.toBatch() instanceof DeltaV2BatchWrite);
  }

  @Test
  public void testToStreaming_returnsDeltaV2StreamingWrite(@TempDir File tempDir) {
    String path = createTable(tempDir, "write_to_streaming");
    DeltaV2Write write = newWrite(path, CaseInsensitiveStringMap.empty());

    assertTrue(write.toStreaming() instanceof DeltaV2StreamingWrite);
  }

  @Test
  public void testToStreaming_rejectsUnsupportedOptions(@TempDir File tempDir) {
    String path = createTable(tempDir, "write_rejects_options");

    for (String option : UNSUPPORTED_STREAMING_OPTIONS) {
      CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(Map.of(option, "x"));
      DeltaV2Write write = newWrite(path, options);
      assertThrows(
          UnsupportedOperationException.class,
          write::toStreaming,
          "streaming write should reject option: " + option);
    }
  }

  private String createTable(File tempDir, String tableName) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tableName, path));
    return path;
  }

  private DeltaV2Write newWrite(String path, CaseInsensitiveStringMap options) {
    Snapshot snapshot =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf())
            .loadLatestSnapshot();
    LogicalWriteInfo info = WriteTestUtils.logicalWriteInfo(TABLE_SCHEMA, options);
    return new DeltaV2Write(
        defaultEngine, spark.sessionState().newHadoopConf(), path, snapshot, TABLE_SCHEMA, info);
  }
}
