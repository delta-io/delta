/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.sink.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.flink.TestHelper;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/** Test suite for {@link DeltaDynamicTableSink}. */
class DeltaDynamicTableSinkTest extends TestHelper {

  @Test
  void testLoadTable() {
    withTempDir(
        dir -> {
          Map<String, String> options = Map.of("connector", "delta", "table_path", dir.getPath());

          ResolvedCatalogTable resolvedTable = simpleTable(options, /* pk = */ null);

          TestDynamicTableSinkContext context = new TestDynamicTableSinkContext(resolvedTable);

          DeltaDynamicTableSinkFactory factory = new DeltaDynamicTableSinkFactory();
          var sink = factory.createDynamicTableSink(context);

          assertTrue(sink instanceof DeltaDynamicTableSink);
          // Default is append mode → insert-only changelog.
          assertEquals(
              ChangelogMode.insertOnly(), sink.getChangelogMode(ChangelogMode.insertOnly()));
        });
  }

  @Test
  void testUpsertModeAdvertisesUpsertChangelogMode() {
    withTempDir(
        dir -> {
          Map<String, String> options = new HashMap<>();
          options.put("connector", "delta");
          options.put("table_path", dir.getPath());
          options.put("write.mode", "upsert");

          ResolvedCatalogTable resolvedTable = simpleTable(options, List.of("id"));

          DynamicTableSink sink =
              new DeltaDynamicTableSinkFactory()
                  .createDynamicTableSink(new TestDynamicTableSinkContext(resolvedTable));

          ChangelogMode mode = sink.getChangelogMode(ChangelogMode.upsert());
          assertTrue(mode.contains(RowKind.INSERT), "upsert mode must allow INSERT");
          assertTrue(mode.contains(RowKind.UPDATE_AFTER), "upsert mode must allow UPDATE_AFTER");
          assertTrue(mode.contains(RowKind.DELETE), "upsert mode must allow DELETE");
          // UPDATE_BEFORE is elided by Flink for PK sinks.
          assertTrue(
              !mode.contains(RowKind.UPDATE_BEFORE),
              "upsert mode should not request UPDATE_BEFORE");
        });
  }

  @Test
  void testUpsertModeWithoutPrimaryKeyThrows() {
    withTempDir(
        dir -> {
          Map<String, String> options = new HashMap<>();
          options.put("connector", "delta");
          options.put("table_path", dir.getPath());
          options.put("write.mode", "upsert");

          ResolvedCatalogTable resolvedTable = simpleTable(options, /* pk = */ null);

          assertThrows(
              ValidationException.class,
              () ->
                  new DeltaDynamicTableSinkFactory()
                      .createDynamicTableSink(new TestDynamicTableSinkContext(resolvedTable)));
        });
  }

  @Test
  void testUpsertModeRejectsUnknownPrimaryKeyOption() {
    withTempDir(
        dir -> {
          // The 'primary_key' option is NOT a public option; the factory's `validate()` call
          // should reject it as an unknown option.
          Map<String, String> options = new HashMap<>();
          options.put("connector", "delta");
          options.put("table_path", dir.getPath());
          options.put("write.mode", "upsert");
          options.put("primary_key", "id");

          ResolvedCatalogTable resolvedTable = simpleTable(options, List.of("id"));

          assertThrows(
              ValidationException.class,
              () ->
                  new DeltaDynamicTableSinkFactory()
                      .createDynamicTableSink(new TestDynamicTableSinkContext(resolvedTable)));
        });
  }

  @Test
  void testNonPhysicalColumnsAreRejected() {
    withTempDir(
        dir -> {
          Map<String, String> options = new HashMap<>();
          options.put("connector", "delta");
          options.put("table_path", dir.getPath());
          options.put("write.mode", "upsert");

          // A virtual (non-physical) metadata column "m" precedes the physical PK column "id".
          // The Delta sink only writes physical columns, and a non-physical column shifts the
          // ordinals the writer/primary-key operate on, so the factory must reject it.
          ResolvedSchema resolvedSchema =
              new ResolvedSchema(
                  Arrays.asList(
                      Column.metadata("m", DataTypes.BIGINT(), null, /* isVirtual = */ true),
                      Column.physical("id", DataTypes.BIGINT().notNull()),
                      Column.physical("dt", DataTypes.STRING())),
                  Collections.emptyList(),
                  UniqueConstraint.primaryKey("pk", List.of("id")));

          CatalogTable table =
              CatalogTable.newBuilder()
                  .schema(
                      Schema.newBuilder()
                          .columnByMetadata("m", DataTypes.BIGINT(), null, /* isVirtual = */ true)
                          .column("id", DataTypes.BIGINT().notNull())
                          .column("dt", DataTypes.STRING())
                          .primaryKey("id")
                          .build())
                  .comment("test table")
                  .partitionKeys(Collections.emptyList())
                  .options(options)
                  .build();

          ResolvedCatalogTable resolvedTable = new ResolvedCatalogTable(table, resolvedSchema);

          assertThrows(
              ValidationException.class,
              () ->
                  new DeltaDynamicTableSinkFactory()
                      .createDynamicTableSink(new TestDynamicTableSinkContext(resolvedTable)));
        });
  }

  /**
   * Build a {@link ResolvedCatalogTable} with two columns {@code (id BIGINT, dt STRING)} and the
   * given options + optional primary-key columns.
   */
  private static ResolvedCatalogTable simpleTable(Map<String, String> options, List<String> pk) {
    Schema.Builder schemaBuilder =
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT().notNull())
            .column("dt", DataTypes.STRING());
    if (pk != null && !pk.isEmpty()) {
      schemaBuilder.primaryKey(pk);
    }

    CatalogTable table =
        CatalogTable.newBuilder()
            .schema(schemaBuilder.build())
            .comment("test table")
            .partitionKeys(Collections.emptyList())
            .options(options)
            .build();

    ResolvedSchema resolvedSchema =
        pk == null || pk.isEmpty()
            ? ResolvedSchema.physical(
                new String[] {"id", "dt"},
                new org.apache.flink.table.types.DataType[] {
                  DataTypes.BIGINT().notNull(), DataTypes.STRING()
                })
            : new ResolvedSchema(
                Arrays.asList(
                    Column.physical("id", DataTypes.BIGINT().notNull()),
                    Column.physical("dt", DataTypes.STRING())),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", pk));
    return new ResolvedCatalogTable(table, resolvedSchema);
  }
}
