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
package io.delta.spark.internal.v2.utils;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Stream;
import org.apache.spark.sql.catalyst.expressions.FileSourceConstantMetadataStructField;
import org.apache.spark.sql.delta.DeltaIllegalStateException;
import org.apache.spark.sql.delta.RowTracking;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.collection.JavaConverters;

public class RowTrackingUtilsTest {

  @Test
  public void testIsEnabled_NotEnabledInMetadata_ReturnsFalse() {
    Protocol protocol = createProtocol(3, 7, Set.of("rowTracking"), Set.of("rowTracking"));
    Metadata metadata = createMetadata(Collections.emptyMap());
    assertFalse(io.delta.kernel.internal.rowtracking.RowTracking.isEnabled(protocol, metadata));
  }

  @Test
  public void testIsEnabled_SupportedAndEnabled_ReturnsTrue() {
    Protocol protocol = createProtocol(3, 7, Set.of("rowTracking"), Set.of("rowTracking"));
    Map<String, String> config = new HashMap<>();
    config.put("delta.enableRowTracking", "true");
    Metadata metadata = createMetadata(config);
    assertTrue(io.delta.kernel.internal.rowtracking.RowTracking.isEnabled(protocol, metadata));
  }

  @Test
  public void testIsEnabled_EnabledButNotSupported_ThrowsError() {
    Protocol protocol = createProtocol(1, 1, Collections.emptySet(), Collections.emptySet());
    Map<String, String> config = new HashMap<>();
    config.put("delta.enableRowTracking", "true");
    Metadata metadata = createMetadata(config);

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> io.delta.kernel.internal.rowtracking.RowTracking.isEnabled(protocol, metadata));
    assertTrue(
        exception
            .getMessage()
            .contains("doesn't support table feature 'delta.feature.rowTracking'"));
  }

  @Test
  public void testCreateMetadataStructFields_NotEnabled_ReturnsEmptyList() {
    Protocol protocol = createProtocol(3, 7, Set.of("rowTracking"), Set.of("rowTracking"));
    Metadata metadata = createMetadata(Collections.emptyMap());
    List<StructField> fields =
        RowTrackingUtils.createMetadataStructFields(protocol, metadata, false, false);
    assertTrue(fields.isEmpty());
  }

  @Test
  public void testCreateMetadataStructFields_MissingMaterializedRowId_ThrowsException() {
    Protocol protocol = createProtocol(3, 7, Set.of("rowTracking"), Set.of("rowTracking"));
    Map<String, String> config = new HashMap<>();
    config.put("delta.enableRowTracking", "true");
    // Missing MATERIALIZED_ROW_ID_COLUMN_NAME
    config.put(TableConfig.MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME.getKey(), "__row_version");
    Metadata metadata = createMetadata(config);

    DeltaIllegalStateException exception =
        assertThrows(
            DeltaIllegalStateException.class,
            () -> RowTrackingUtils.createMetadataStructFields(protocol, metadata, false, false));
    assertTrue(exception.getMessage().contains("Row ID"));
  }

  @Test
  public void testCreateMetadataStructFields_MissingMaterializedRowCommitVersion_ThrowsException() {
    Protocol protocol = createProtocol(3, 7, Set.of("rowTracking"), Set.of("rowTracking"));
    Map<String, String> config = new HashMap<>();
    config.put("delta.enableRowTracking", "true");
    config.put(TableConfig.MATERIALIZED_ROW_ID_COLUMN_NAME.getKey(), "__row_id");
    // Missing MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME
    Metadata metadata = createMetadata(config);

    DeltaIllegalStateException exception =
        assertThrows(
            DeltaIllegalStateException.class,
            () -> RowTrackingUtils.createMetadataStructFields(protocol, metadata, false, false));
    assertTrue(exception.getMessage().contains("Row Commit Version"));
  }

  private static Stream<Arguments> createMetadataStructFieldsTestProvider() {
    // Note: withMaterializedColumns must be true when row tracking is enabled
    // because materialized column names are required
    return Stream.of(
        // nullableConstant, nullableGenerated
        Arguments.of(false, false),
        Arguments.of(false, true),
        Arguments.of(true, false),
        Arguments.of(true, true));
  }

  @ParameterizedTest
  @MethodSource("createMetadataStructFieldsTestProvider")
  public void testCreateMetadataStructFields(boolean nullableConstant, boolean nullableGenerated) {
    // Create Kernel Protocol and Metadata
    // Note: materialized columns are always configured when row tracking is enabled
    Protocol kernelProtocol = createProtocol(3, 7, Set.of("rowTracking"), Set.of("rowTracking"));
    Map<String, String> config = new HashMap<>();
    config.put("delta.enableRowTracking", "true");
    config.put(TableConfig.MATERIALIZED_ROW_ID_COLUMN_NAME.getKey(), "__row_id");
    config.put(TableConfig.MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME.getKey(), "__row_version");
    Metadata kernelMetadata = createMetadata(config);

    // Get actual result from Kernel-Spark API
    List<StructField> actualFields =
        RowTrackingUtils.createMetadataStructFields(
            kernelProtocol, kernelMetadata, nullableConstant, nullableGenerated);

    // Build expected fields
    List<StructField> expectedFields = new ArrayList<>();
    // row_id (generated field)
    expectedFields.add(
        new StructField(
            "row_id",
            DataTypes.LongType,
            nullableGenerated,
            new MetadataBuilder()
                .withMetadata(
                    org.apache.spark.sql.catalyst.expressions.FileSourceGeneratedMetadataStructField
                        .metadata("row_id", "__row_id"))
                .putBoolean("__row_id_metadata_col", true)
                .build()));
    // base_row_id (constant field)
    expectedFields.add(
        new StructField(
            "base_row_id",
            DataTypes.LongType,
            nullableConstant,
            new MetadataBuilder()
                .withMetadata(FileSourceConstantMetadataStructField.metadata("base_row_id"))
                .putBoolean("__base_row_id_metadata_col", true)
                .build()));
    // default_row_commit_version (constant field)
    expectedFields.add(
        new StructField(
            "default_row_commit_version",
            DataTypes.LongType,
            nullableConstant,
            new MetadataBuilder()
                .withMetadata(
                    FileSourceConstantMetadataStructField.metadata("default_row_commit_version"))
                .putBoolean("__default_row_version_metadata_col", true)
                .build()));
    // row_commit_version (generated field)
    expectedFields.add(
        new StructField(
            "row_commit_version",
            DataTypes.LongType,
            nullableGenerated,
            new MetadataBuilder()
                .withMetadata(
                    org.apache.spark.sql.catalyst.expressions.FileSourceGeneratedMetadataStructField
                        .metadata("row_commit_version", "__row_version"))
                .putBoolean("__row_commit_version_metadata_col", true)
                .build()));

    String protocolJson =
        JsonUtils.rowToJson(SingleAction.createProtocolSingleAction(kernelProtocol.toRow()));
    org.apache.spark.sql.delta.actions.Protocol sparkV1Protocol =
        Action.fromJson(protocolJson).wrap().protocol();
    String metadataJson =
        JsonUtils.rowToJson(SingleAction.createMetadataSingleAction(kernelMetadata.toRow()));
    org.apache.spark.sql.delta.actions.Metadata sparkV1Metadata =
        Action.fromJson(metadataJson).wrap().metaData();
    scala.collection.Iterable<StructField> sparkV1FieldsIterable =
        RowTracking.createMetadataStructFields(
            sparkV1Protocol, sparkV1Metadata, nullableConstant, nullableGenerated);
    List<StructField> v1Fields =
        new ArrayList<>(JavaConverters.asJavaCollection(sparkV1FieldsIterable));

    assertEquals(expectedFields, actualFields);
    // Ensure both delta implementation return same result.
    assertEquals(v1Fields, actualFields);
  }

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
            return null;
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
}
