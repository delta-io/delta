/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.spark.utils;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Stream;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RowTrackingUtilsTest {

  private Protocol createProtocol(
      int minReaderVersion,
      int minWriterVersion,
      Set<String> readerFeatures,
      Set<String> writerFeatures) {
    return new Protocol(minReaderVersion, minWriterVersion, readerFeatures, writerFeatures);
  }

  private Metadata createMetadata(Map<String, String> configuration) {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    ArrayValue emptyPartitionColumns =
        new ArrayValue() {
          @Override
          public int getSize() {
            return 0;
          }

          @Override
          public ColumnVector getElements() {
            throw new UnsupportedOperationException("Empty array has no elements");
          }
        };
    return new Metadata(
        "id",
        Optional.empty() /* name */,
        Optional.empty() /* description */,
        new Format(),
        schema.toJson(),
        schema,
        emptyPartitionColumns,
        Optional.empty() /* createdTime */,
        io.delta.kernel.internal.util.VectorUtils.stringStringMapValue(configuration));
  }

  @Test
  public void testIsEnabled_NotEnabledInMetadata_ReturnsFalse() {
    Protocol protocol = createProtocol(3, 7, Set.of("rowTracking"), Set.of("rowTracking"));
    Metadata metadata = createMetadata(Collections.emptyMap());
    assertFalse(RowTrackingUtils.isEnabled(protocol, metadata));
  }

  @Test
  public void testIsEnabled_SupportedAndEnabled_ReturnsTrue() {
    Protocol protocol = createProtocol(3, 7, Set.of("rowTracking"), Set.of("rowTracking"));
    Map<String, String> config = new HashMap<>();
    config.put("delta.enableRowTracking", "true");
    Metadata metadata = createMetadata(config);
    assertTrue(RowTrackingUtils.isEnabled(protocol, metadata));
  }

  @Test
  public void testIsEnabled_EnabledButNotSupported_ThrowsError() {
    Protocol protocol = createProtocol(1, 1, Collections.emptySet(), Collections.emptySet());
    Map<String, String> config = new HashMap<>();
    config.put("delta.enableRowTracking", "true");
    Metadata metadata = createMetadata(config);

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class, () -> RowTrackingUtils.isEnabled(protocol, metadata));
    assertTrue(exception.getMessage().contains("doesn't support the 'rowTracking' table feature"));
  }

  @Test
  public void testCreateMetadataStructFields_NotEnabled_ReturnsEmptyList() {
    Protocol protocol = createProtocol(3, 7, Set.of("rowTracking"), Set.of("rowTracking"));
    Metadata metadata = createMetadata(Collections.emptyMap());
    List<StructField> fields =
        RowTrackingUtils.createMetadataStructFields(protocol, metadata, false, false);
    assertTrue(fields.isEmpty());
  }

  private static Stream<Arguments> nullableFieldConfigProvider() {
    return Stream.of(
        Arguments.of(false, false),
        Arguments.of(false, true),
        Arguments.of(true, false),
        Arguments.of(true, true));
  }

  @ParameterizedTest
  @MethodSource("nullableFieldConfigProvider")
  public void testCreateMetadataStructFields_VariousNullableConfigs_CreatesCorrectFields(
      boolean nullableConstant, boolean nullableGenerated) {
    Protocol protocol = createProtocol(3, 7, Set.of("rowTracking"), Set.of("rowTracking"));
    Map<String, String> config = new HashMap<>();
    config.put("delta.enableRowTracking", "true");
    Metadata metadata = createMetadata(config);

    List<StructField> actualFields =
        RowTrackingUtils.createMetadataStructFields(
            protocol, metadata, nullableConstant, nullableGenerated);

    // Construct expected fields
    List<StructField> expectedFields =
        Arrays.asList(
            new StructField(
                "base_row_id",
                DataTypes.LongType,
                nullableConstant,
                new MetadataBuilder()
                    .putString("__metadata_type", "constant")
                    .putBoolean("__base_row_id_metadata_col", true)
                    .build()),
            new StructField(
                "default_row_commit_version",
                DataTypes.LongType,
                nullableConstant,
                new MetadataBuilder()
                    .putString("__metadata_type", "constant")
                    .putBoolean("__default_row_version_metadata_col", true)
                    .build()),
            new StructField(
                "row_id",
                DataTypes.LongType,
                nullableGenerated,
                new MetadataBuilder()
                    .putString("__metadata_type", "generated")
                    .putBoolean("__row_id_metadata_col", true)
                    .build()),
            new StructField(
                "row_commit_version",
                DataTypes.LongType,
                nullableGenerated,
                new MetadataBuilder()
                    .putString("__metadata_type", "generated")
                    .putBoolean("__row_commit_version_metadata_col", true)
                    .build()));

    assertEquals(expectedFields, actualFields);
  }
}
