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

package io.delta.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.delta.flink.MockHttp;
import io.delta.flink.TestHelper;
import io.delta.kernel.types.*;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for UnityCatalog. */
class UnityCatalogTest extends TestHelper {

  private static final URI CATALOG_ENDPOINT =
      URI.create("https://e2-dogfood.staging.cloud.databricks.com/");
  private static final String CATALOG_TOKEN = "";

  @Test
  void testGetTable() {
    withTempDir(
        dir ->
            MockHttp.withMock(
                MockHttp.forExistingUCTable(dir.getAbsolutePath()),
                mockHttp -> {
                  UnityCatalog uc = new UnityCatalog("main", mockHttp.uri(), "");
                  uc.open();

                  DeltaCatalog.TableDescriptor tableDescriptor = uc.getTable("dummy");
                  assertEquals(
                      AbstractKernelTable.normalize(URI.create(dir.getAbsolutePath())),
                      tableDescriptor.tablePath);
                }));
  }

  @Disabled("Requires Unity Catalog access")
  @Test
  void testGetTableFromDogfood() {
    UnityCatalog catalog = new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN);
    catalog.open();
    DeltaCatalog.TableDescriptor table = catalog.getTable("main.hao.testcreatewrite");
    assertEquals("", table.tablePath);
  }

  @Disabled("Requires Unity Catalog access")
  @Test
  void testCreateTableFromDogfood() {
    UnityCatalog catalog = new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN);
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

    catalog.createTable(
        "main.hao.testcreate", schema, Collections.emptyList(), Map.of("a", "b"), (uri) -> {});

    DeltaCatalog.TableDescriptor tableInfo = catalog.getTable("main.hao.testcreate");
    assertNotNull(tableInfo.tablePath);
  }
}
