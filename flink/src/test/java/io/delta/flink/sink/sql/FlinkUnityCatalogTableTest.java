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

import static org.junit.jupiter.api.Assertions.*;

import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.Test;

/** Test suite for {@link FlinkUnityCatalogTable}. */
class FlinkUnityCatalogTableTest {

  @Test
  void testBuildSchema() {
    List<ColumnInfo> colInfos =
        List.of(
            new ColumnInfo()
                .name("id")
                .typeJson(
                    "{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}"),
            new ColumnInfo()
                .name("name")
                .typeJson(
                    "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}"),
            new ColumnInfo()
                .name("bin")
                .typeJson(
                    "{\"name\":\"bin\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}"),
            new ColumnInfo()
                .name("de")
                .typeJson(
                    "{\"name\":\"de\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}}"),
            new ColumnInfo()
                .name("str")
                .typeJson(
                    "{\"name\":\"str\",\"type\":{\"type\":\"struct\","
                        + "\"fields\":[{\"name\":\"nested\",\"type\":\"integer\",\"nullable\":true,"
                        + "\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}"),
            new ColumnInfo()
                .name("sl")
                .typeJson(
                    "{\"name\":\"sl\",\"type\":{\"type\":\"array\","
                        + "\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}"),
            new ColumnInfo()
                .name("cl")
                .typeJson(
                    "{\"name\":\"cl\",\"type\":{\"type\":\"array\","
                        + "\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"n\",\"type\":\"integer\","
                        + "\"nullable\":true,\"metadata\":{}}]},\"containsNull\":true},\"nullable\":true,"
                        + "\"metadata\":{}}"),
            new ColumnInfo()
                .name("ml")
                .typeJson(
                    "{\"name\":\"ml\",\"type\":{\"type\":\"map\","
                        + "\"keyType\":\"integer\",\"valueType\":\"string\",\"valueContainsNull\":true},"
                        + "\"nullable\":true,\"metadata\":{}}"),
            new ColumnInfo()
                .name("md")
                .typeJson(
                    "{\"name\":\"md\",\"type\":{\"type\":\"map\","
                        + "\"keyType\":\"string\",\"valueType\":{\"type\":\"struct\",\"fields\":[{\"name\":"
                        + "\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]},"
                        + "\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}}"),
            new ColumnInfo()
                .name("dt")
                .typeJson("{\"name\":\"dt\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}"),
            new ColumnInfo()
                .name("ts")
                .typeJson(
                    "{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}"));

    Schema schema = FlinkUnityCatalogTable.buildSchema(colInfos);
    assertEquals(11, schema.getColumns().size());
    assertEquals("bin", schema.getColumns().get(2).getName());
    assertTrue(
        ((UnresolvedPhysicalColumn) schema.getColumns().get(2)).getDataType()
            instanceof AtomicDataType);
    assertTrue(
        ((AtomicDataType) ((UnresolvedPhysicalColumn) schema.getColumns().get(2)).getDataType())
                .getLogicalType()
            instanceof VarBinaryType);
    assertTrue(
        ((UnresolvedPhysicalColumn) schema.getColumns().get(8)).getDataType()
            instanceof KeyValueDataType);
    assertTrue(
        ((KeyValueDataType) ((UnresolvedPhysicalColumn) schema.getColumns().get(8)).getDataType())
                .getLogicalType()
            instanceof MapType);
  }

  @Test
  void testNarrowIntegerTypes() {
    assertEquals(
        new TinyIntType(true),
        FlinkUnityCatalogTable.fromJson("{\"type\":\"byte\",\"nullable\":true}").getLogicalType());
    assertEquals(
        new SmallIntType(false),
        FlinkUnityCatalogTable.fromJson("{\"type\":\"short\",\"nullable\":false}")
            .getLogicalType());
  }

  @Test
  void testResolveTableWithVoidColumn() throws Exception {
    TableInfo tableInfo =
        new TableInfo()
            .name("events")
            .columns(
                List.of(
                    column("id"),
                    new ColumnInfo()
                        .name("pending")
                        .typeJson(
                            "{\"name\":\"pending\",\"type\":\"void\",\"nullable\":true,"
                                + "\"metadata\":{}}")))
            .dataSourceFormat(DataSourceFormat.DELTA)
            .properties(Map.of());
    FlinkUnityCatalogTable table =
        new FlinkUnityCatalogTable(tableInfo, URI.create("https://example.com"), "token");
    GenericInMemoryCatalog catalog = new GenericInMemoryCatalog("test_catalog");
    catalog.createTable(new ObjectPath("default", "events"), table, false);
    TableEnvironment tableEnvironment =
        TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    tableEnvironment.registerCatalog("test_catalog", catalog);

    assertEquals(
        new NullType(),
        tableEnvironment
            .from("`test_catalog`.`default`.`events`")
            .getResolvedSchema()
            .getColumnDataTypes()
            .get(1)
            .getLogicalType());
  }

  @Test
  void testPartitionMetadata() {
    List<ColumnInfo> columns =
        List.of(column("id"), column("day").partitionIndex(1), column("region").partitionIndex(0));
    TableInfo tableInfo =
        new TableInfo()
            .name("events")
            .columns(columns)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .properties(Map.of());

    FlinkUnityCatalogTable table =
        new FlinkUnityCatalogTable(tableInfo, URI.create("https://example.com"), "token");

    assertTrue(table.isPartitioned());
    assertEquals(List.of("region", "day"), table.getPartitionKeys());
    assertEquals("region,day", table.getOptions().get("partitions"));

    CatalogTable copiedTable = (CatalogTable) table.copy();
    assertEquals(table.getPartitionKeys(), copiedTable.getPartitionKeys());
    assertEquals(table.getPartitionKeys(), table.copy(Map.of()).getPartitionKeys());
  }

  private static ColumnInfo column(String name) {
    return new ColumnInfo()
        .name(name)
        .typeJson(
            String.format(
                "{\"name\":\"%s\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}", name));
  }
}
