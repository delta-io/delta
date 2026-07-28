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

package io.delta.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.flink.MockHttp;
import io.delta.flink.TestHelper;
import io.delta.kernel.types.*;
import io.delta.kernel.unitycatalog.UCTableIdentifier;
import java.net.URI;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for UnityCatalog. */
class UnityCatalogTest extends TestHelper {

  @Test
  void testToUcTableIdentifier() {
    UnityCatalog catalog = new UnityCatalog("main", URI.create("http://localhost"), "");

    UCTableIdentifier twoPartIdentifier = catalog.toUcTableIdentifier("default.tbl");
    assertEquals("main", twoPartIdentifier.getCatalogName());
    assertEquals("default", twoPartIdentifier.getSchemaName());
    assertEquals("tbl", twoPartIdentifier.getTableName());

    UCTableIdentifier threePartIdentifier = catalog.toUcTableIdentifier("main.default.tbl");
    assertEquals("main", threePartIdentifier.getCatalogName());
    assertEquals("default", threePartIdentifier.getSchemaName());
    assertEquals("tbl", threePartIdentifier.getTableName());
  }

  @Test
  void testToUcTableIdentifierRejectsInvalidNames() {
    UnityCatalog catalog = new UnityCatalog("main", URI.create("http://localhost"), "");

    assertThrows(IllegalArgumentException.class, () -> catalog.toUcTableIdentifier("tbl"));
    assertThrows(
        IllegalArgumentException.class, () -> catalog.toUcTableIdentifier("other.default.tbl"));
  }

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
}
