/*
 *  Copyright (2021) The Delta Lake Project Authors.
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
import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.MapType;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for {@link FlinkUnityCatalogTable}. */
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
            instanceof BinaryType);
    assertTrue(
        ((UnresolvedPhysicalColumn) schema.getColumns().get(8)).getDataType()
            instanceof KeyValueDataType);
    assertTrue(
        ((KeyValueDataType) ((UnresolvedPhysicalColumn) schema.getColumns().get(8)).getDataType())
                .getLogicalType()
            instanceof MapType);
  }
}
