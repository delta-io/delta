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

package io.delta.flink.inttest;

import static io.delta.flink.inttest.IntTestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import io.delta.flink.table.AbstractKernelTable;
import io.delta.flink.table.DeltaCatalog;
import io.delta.flink.table.UnityCatalog;
import io.delta.kernel.types.*;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableInfo;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class UnityCatalogIntTest extends IntTestBase {

  static String TEST_TABLE_NAME = "main.hao.flinkint_ucread";
  static String TEST_NEW_TABLE_NAME = "main.hao.flinkint_uccreate";

  public UnityCatalogIntTest(SparkSession spark, URI catalogEndpoint, String catalogToken) {
    super(spark, catalogEndpoint, catalogToken);
  }

  static void assertColumn(StructField field, ColumnInfo column) {
    assertEquals(field.getName(), column.getName());

    var nameMapping =
        Map.of("byte", "tinyint", "short", "smallint", "integer", "int", "long", "bigint");

    var origin = typeToDDL(field.getDataType()).replace(" ", "");
    var normalized = origin;
    for (Map.Entry<String, String> e : nameMapping.entrySet()) {
      normalized = normalized.replace(e.getKey(), e.getValue());
    }
    var expected = column.getTypeText().toLowerCase(Locale.ROOT);
    assertTrue(expected.equals(origin) || expected.equals(normalized));
  }

  @BeforeEach
  public void before() {
    spark.sql(String.format("DROP TABLE IF EXISTS %s", TEST_TABLE_NAME));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", TEST_NEW_TABLE_NAME));
    spark.sql(
        String.format(
            "CREATE TABLE %s (%s) USING delta TBLPROPERTIES ('delta.feature.catalogManaged'= 'supported')",
            TEST_TABLE_NAME, schemaToDDL(SCHEMA_WITH_ALL_TYPES)));
  }

  @AfterEach
  public void after() {}

  @IntTest
  void testGetTable() {
    UnityCatalog catalog = new UnityCatalog("main", catalogEndpoint, catalogToken);
    catalog.open();
    DeltaCatalog.TableDescriptor table = catalog.getTable(TEST_TABLE_NAME);
    assertNotNull(table.tablePath);
  }

  @IntTest
  void testCreateTable() {
    UnityCatalog catalog = new UnityCatalog("main", catalogEndpoint, catalogToken);
    catalog.open();
    StructType nested =
        new StructType().add("nested", IntegerType.INTEGER).add("nested_id", StringType.STRING);

    StructType schema =
        new StructType()
            .add("id", IntegerType.INTEGER)
            .add("name", StringType.STRING)
            .add("b", BooleanType.BOOLEAN, true)
            .add("i", IntegerType.INTEGER, true)
            .add("l", LongType.LONG, true)
            .add("f", FloatType.FLOAT, true)
            .add("d", DoubleType.DOUBLE, true)
            .add("s", StringType.STRING, true)
            .add("bin", BinaryType.BINARY, true)
            .add("dec", new DecimalType(10, 2), true)
            .add("ids", new ArrayType(IntegerType.INTEGER, true), true)
            .add("names", new ArrayType(nested, true), true)
            .add("map", new MapType(IntegerType.INTEGER, StringType.STRING, true), true)
            .add("map2", new MapType(IntegerType.INTEGER, nested, true), true);

    AtomicReference<URI> storagePath = new AtomicReference<>();
    AtomicReference<String> uuid = new AtomicReference<>();
    catalog.createTable(
        TEST_NEW_TABLE_NAME,
        schema,
        Collections.emptyList(),
        Map.of("a", "b"),
        (desc) -> {
          storagePath.set(desc.tablePath);
          uuid.set(desc.uuid);
        });

    DeltaCatalog.TableDescriptor tableDesc = catalog.getTable(TEST_NEW_TABLE_NAME);
    assertEquals(tableDesc.uuid, uuid.get());
    assertEquals(tableDesc.tablePath, AbstractKernelTable.normalize(storagePath.get()));
    TableInfo detail = catalog.getTableDetail(TEST_NEW_TABLE_NAME);

    for (int i = 0; i < schema.fields().size(); i++) {
      assertColumn(schema.fields().get(i), detail.getColumns().get(i));
    }
  }

  @IntTest
  void testGetTableDetail() {
    UnityCatalog catalog = new UnityCatalog("main", catalogEndpoint, catalogToken);
    catalog.open();

    TableInfo detail = catalog.getTableDetail(TEST_TABLE_NAME);

    var schema = SCHEMA_WITH_ALL_TYPES;
    assertEquals(schema.fields().size(), Objects.requireNonNull(detail.getColumns()).size());
    for (int i = 0; i < schema.fields().size(); i++) {
      assertColumn(schema.fields().get(i), detail.getColumns().get(i));
    }
  }

  @IntTest
  public void testListTables() {
    UnityCatalog catalog = new UnityCatalog("main", catalogEndpoint, catalogToken);
    catalog.open();

    List<String> tables = catalog.listTables("hao");
    assertTrue(tables.contains("flinkint_ucread"));
  }

  @IntTest
  public void testSchema() {
    UnityCatalog catalog = new UnityCatalog("main", catalogEndpoint, catalogToken);
    catalog.open();

    List<String> schemas = catalog.listSchemas();
    assertEquals(schemas.size(), 1000);

    SchemaInfo schemaInfo = catalog.getSchema("hao");
    assertEquals(schemaInfo.getCatalogName(), "main");
    assertEquals(schemaInfo.getFullName(), "main.hao");
  }
}
