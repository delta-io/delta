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
package io.delta.kernel.internal.util;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.assertj.core.util.Maps;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_ID_KEY;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_ID;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_KEY;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_NAME;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_NONE;

public class TestColumnMapping {

    @Test
    public void columnMappingEnabled() {
        assertThat(ColumnMapping.isColumnMappingModeEnabled(null)).isFalse();
        assertThat(ColumnMapping.isColumnMappingModeEnabled("null")).isFalse();
        assertThat(ColumnMapping.isColumnMappingModeEnabled("none")).isFalse();
        assertThat(ColumnMapping.isColumnMappingModeEnabled("NaMe")).isFalse();
        assertThat(ColumnMapping.isColumnMappingModeEnabled("name")).isTrue();
        assertThat(ColumnMapping.isColumnMappingModeEnabled("id")).isTrue();
    }

    @Test
    public void columnMappingChangeWithEmptyConfig() {
        assertThatNoException()
                .isThrownBy(() ->
                        ColumnMapping.verifyColumnMappingChange(
                                Collections.emptyMap(),
                                Collections.emptyMap()));
    }

    @Test
    public void columnMappingModeChangeIsAllowed() {
        assertThatNoException()
                .isThrownBy(() ->
                        ColumnMapping.verifyColumnMappingChange(
                                Collections.emptyMap(),
                                Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME)
                        ));

        assertThatNoException()
                .isThrownBy(() ->
                        ColumnMapping.verifyColumnMappingChange(
                                Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NONE),
                                Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME)
                        ));

        assertThatNoException()
                .isThrownBy(() ->
                        ColumnMapping.verifyColumnMappingChange(
                                Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_ID),
                                Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_ID)
                        ));

        assertThatNoException()
                .isThrownBy(() ->
                        ColumnMapping.verifyColumnMappingChange(
                                Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME),
                                Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME)
                        ));

    }

    @Test
    public void columnMappingModeChangeNotAllowedOnExistingTable() {
        boolean isNewTable = false;
        assertThatThrownBy(() ->
                ColumnMapping.verifyColumnMappingChange(
                        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME),
                        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_ID),
                        isNewTable
                ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Changing column mapping mode from 'name' to 'id' is not supported");

        assertThatThrownBy(() ->
                ColumnMapping.verifyColumnMappingChange(
                        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_ID),
                        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME),
                        isNewTable
                ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Changing column mapping mode from 'id' to 'name' is not supported");

        assertThatThrownBy(() ->
                ColumnMapping.verifyColumnMappingChange(
                        Collections.emptyMap(),
                        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_ID),
                        isNewTable
                ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Changing column mapping mode from 'none' to 'id' is not supported");
    }

    @Test
    public void findMaxColumnId() {
        assertThat(ColumnMapping.findMaxColumnId(new StructType()))
                .isEqualTo(0);

        assertThat(ColumnMapping.findMaxColumnId(
                new StructType()
                        .add("a", StringType.STRING, true)
                        .add("b", IntegerType.INTEGER, true)))
                .isEqualTo(0);

        assertThat(ColumnMapping.findMaxColumnId(
                new StructType()
                        .add("a", StringType.STRING, FieldMetadata.builder()
                                .putLong(COLUMN_MAPPING_ID_KEY, 14).build())
                        .add("b", IntegerType.INTEGER, FieldMetadata.builder()
                                .putLong(COLUMN_MAPPING_ID_KEY, 17).build())
                        .add("c", IntegerType.INTEGER, FieldMetadata.builder()
                                .putLong(COLUMN_MAPPING_ID_KEY, 3).build())))
                .isEqualTo(17);

        // nested columns are currently not supported
        assertThat(ColumnMapping.findMaxColumnId(
                new StructType()
                        .add("a", StringType.STRING, FieldMetadata.builder()
                                .putLong(COLUMN_MAPPING_ID_KEY, 14).build())
                        .add("b", new StructType()
                                        .add("d", IntegerType.INTEGER, true)
                                        .add("e", IntegerType.INTEGER, true),
                                FieldMetadata.builder()
                                        .putLong(COLUMN_MAPPING_ID_KEY, 15).build())
                        .add("c", IntegerType.INTEGER, FieldMetadata.builder()
                                .putLong(COLUMN_MAPPING_ID_KEY, 7).build())))
                .isEqualTo(15);
    }

    @Test
    public void assignIdAndPhysicalNameOnNewTable() {
        StructType schema = new StructType()
                .add("a", StringType.STRING, true);

        Metadata metadata = ColumnMapping
                .updateColumnMappingMetadata(createMetadata(schema), "id", true);
        Map<String, Object> fieldMetadata = metadata.getSchema().get("a")
                .getMetadata().getEntries();

        assertThat(fieldMetadata)
                .containsEntry(ColumnMapping.COLUMN_MAPPING_ID_KEY, 1L)
                .hasEntrySatisfying(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
                        k -> assertThat(k).asString().startsWith("col-"));
    }

    @Test
    public void assignIdAndPhysicalNameOnUpdatedTable() {
        StructType schema = new StructType()
                .add("a", StringType.STRING, true);

        Metadata metadata = ColumnMapping
                .updateColumnMappingMetadata(createMetadata(schema), "id", false);
        Map<String, Object> fieldMetadata = metadata.getSchema().get("a")
                .getMetadata().getEntries();

        assertThat(fieldMetadata)
                .containsEntry(ColumnMapping.COLUMN_MAPPING_ID_KEY, 1L)
                // the display name is used as the physical name
                .hasEntrySatisfying(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
                        k -> assertThat(k).asString().isEqualTo("a"));
    }

    @Test
    public void noneMappingModeReturnsOriginalSchema() {
        StructType schema = new StructType()
                .add("a", StringType.STRING, true);

        Metadata metadata = ColumnMapping
                .updateColumnMappingMetadata(createMetadata(schema), "none", true);
        assertThat(metadata.getSchema()).isEqualTo(schema);
    }

    @Test
    public void invalidMappingMode() {
        assertThatThrownBy(() -> ColumnMapping
                .updateColumnMappingMetadata(
                        createMetadata(new StructType()), "invalid", true))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Unsupported column mapping mode: invalid");
    }

    private Metadata createMetadata(StructType schema) {
        return new Metadata(UUID.randomUUID().toString(),
                Optional.empty(),
                Optional.empty(),
                new Format(),
                schema.toJson(),
                schema,
                VectorUtils.stringArrayValue(Collections.emptyList()),
                Optional.empty(),
                VectorUtils.stringStringMapValue(Collections.emptyMap()));
    }

    @Test
    public void assignIdAndPhysicalPreservesFieldMetadata() {
        StructType schema = new StructType()
                .add("a", StringType.STRING, FieldMetadata.builder()
                        .putString("key1", "val1")
                        .putString("key2", "val2")
                        .build());

        Metadata metadata = ColumnMapping
                .updateColumnMappingMetadata(createMetadata(schema), "id", true);
        Map<String, Object> fieldMetadata = metadata.getSchema().get("a")
                .getMetadata().getEntries();

        assertThat(fieldMetadata)
                .containsEntry("key1", "val1")
                .containsEntry("key2", "val2")
                .containsEntry(ColumnMapping.COLUMN_MAPPING_ID_KEY, 1L)
                // the display name is used as the physical name
                .hasEntrySatisfying(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
                        k -> assertThat(k).asString().startsWith("col-"));
    }
}
