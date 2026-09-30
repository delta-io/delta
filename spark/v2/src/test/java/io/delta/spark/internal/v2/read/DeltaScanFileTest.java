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

import static org.junit.jupiter.api.Assertions.*;

import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.delta.DeltaParquetFileFormat;
import org.apache.spark.sql.delta.RowIndexFilterType;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

public class DeltaScanFileTest {

  private static final String TABLE_PATH = "file:/tmp/table%20space%25/";

  @Test
  public void testPartitionedFilePreservesPathsAndPartitionValues() {
    StructType schema =
        new StructType()
            .add(
                "logical_name",
                DataTypes.StringType,
                true,
                new MetadataBuilder()
                    .putString("delta.columnMapping.physicalName", "physical_name")
                    .build())
            .add("number", DataTypes.IntegerType)
            .add("timestamp", DataTypes.TimestampType)
            .add("explicit_null", DataTypes.LongType)
            .add("omitted", DataTypes.StringType);
    Map<String, String> values = new HashMap<>();
    values.put("physical_name", "%20/%25/a%2Fb");
    values.put("number", "42");
    values.put("timestamp", "2026-09-30 12:34:56.123456");
    values.put("explicit_null", null);
    AddFile addFile = createAddFile("p=%2520/part%20one%25.parquet", values, null, false);

    PartitionedFile actual = assertMatchesV1(addFile, schema, ZoneId.of("America/Los_Angeles"));
    assertEquals(
        "/tmp/table space%/p=%20/part one%.parquet", actual.filePath().toPath().toUri().getPath());
    InternalRow row = actual.partitionValues();
    assertEquals("%20/%25/a%2Fb", row.getUTF8String(0).toString());
    assertEquals(42, row.getInt(1));
    Instant instant = Instant.parse("2026-09-30T19:34:56.123456Z");
    assertEquals(instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000, row.getLong(2));
    assertTrue(row.isNullAt(3));
    assertTrue(row.isNullAt(4));
  }

  @Test
  public void testPartitionedFilePreservesDeletionVectorsAndRowTracking() {
    DeletionVectorDescriptor[] deletionVectors = {
      null,
      new DeletionVectorDescriptor(
          "u", "00000000000000000000", Option.apply(4), 40, 3L, Option.empty()),
      new DeletionVectorDescriptor(
          "p", "file:/tmp/dv%20file.bin", Option.empty(), 40, 3L, Option.empty()),
      new DeletionVectorDescriptor("i", "00000", Option.empty(), 4, 1L, Option.empty())
    };
    for (DeletionVectorDescriptor dv : deletionVectors) {
      for (boolean rowTracking : new boolean[] {false, true}) {
        AddFile addFile = createAddFile("part.parquet", Collections.emptyMap(), dv, rowTracking);
        PartitionedFile actual = assertMatchesV1(addFile, new StructType(), ZoneId.of("UTC"));
        Map<String, Object> metadata =
            CollectionConverters.asJava(actual.otherConstantMetadataColumnValues());
        if (dv != null) {
          assertEquals(
              dv.serializeToBase64(),
              metadata.get(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED()));
          assertEquals(
              RowIndexFilterType.IF_CONTAINED,
              metadata.get(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE()));
        }
        if (rowTracking) {
          assertEquals(100L, metadata.get("base_row_id"));
          assertEquals(7L, metadata.get("default_row_commit_version"));
        }
        assertEquals((dv == null ? 0 : 2) + (rowTracking ? 2 : 0), metadata.size());
      }
    }
  }

  private static PartitionedFile assertMatchesV1(
      AddFile addFile, StructType schema, ZoneId zoneId) {
    PartitionedFile expected =
        PartitionUtils.buildPartitionedFile(addFile, schema, TABLE_PATH, zoneId);
    PartitionedFile actual =
        PartitionUtils.buildPartitionedFile(
            DeltaScanFile.fromV1AddFile(addFile), schema, TABLE_PATH, zoneId);
    assertEquals(expected.filePath(), actual.filePath());
    assertEquals(expected.partitionValues(), actual.partitionValues());
    assertEquals(expected.start(), actual.start());
    assertEquals(expected.length(), actual.length());
    assertEquals(expected.fileSize(), actual.fileSize());
    assertEquals(expected.modificationTime(), actual.modificationTime());
    assertEquals(
        expected.otherConstantMetadataColumnValues(), actual.otherConstantMetadataColumnValues());
    return actual;
  }

  private static AddFile createAddFile(
      String path,
      Map<String, String> partitionValues,
      DeletionVectorDescriptor deletionVector,
      boolean rowTracking) {
    return new AddFile(
        path,
        ScalaUtils.toScalaMap(partitionValues),
        1024L,
        1234L,
        true,
        null,
        null,
        deletionVector,
        rowTracking ? Option.apply(100L) : Option.empty(),
        rowTracking ? Option.apply(7L) : Option.empty(),
        Option.empty(),
        Option.empty(),
        Option.empty());
  }
}
