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

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.flink.MockHttp;
import io.delta.flink.TestHelper;
import io.delta.kernel.types.*;
import io.delta.kernel.unitycatalog.UCTableIdentifier;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for UnityCatalog. */
class UnityCatalogTest extends TestHelper {

  @Test
  void testCatalogBrowsingSetsIncludeBrowseFalse() {
    String schemasPath = "/api/2.1/unity-catalog/schemas";
    String tablesPath = "/api/2.1/unity-catalog/tables";
    String schemaPath = schemasPath + "/main.default";
    String tablePath = tablesPath + "/main.default.tbl";
    MockHttp.withMock(
        new MockHttp(
            Map.of(
                schemasPath, "{\"schemas\":[{\"name\":\"default\"}]}",
                schemaPath, "{\"name\":\"default\"}",
                tablesPath, "{\"tables\":[{\"name\":\"tbl\"}]}",
                tablePath, "{\"name\":\"tbl\"}"),
            Map.of()),
        mock -> {
          UnityCatalog catalog = new UnityCatalog("main", mock.uri(), "");
          catalog.open();

          assertEquals(List.of("default"), catalog.listSchemas());
          assertEquals("default", catalog.getSchema("default").getName());
          assertEquals(List.of("tbl"), catalog.listTables("default"));
          assertEquals("tbl", catalog.getTableDetail("main.default.tbl").getName());

          for (String path : List.of(schemasPath, schemaPath, tablesPath, tablePath)) {
            mock.verify(
                getRequestedFor(urlPathEqualTo(path))
                    .withQueryParam("include_browse", equalTo("false")));
          }
        });
  }

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
        dir -> {
          String tableId = UUID.randomUUID().toString();
          MockHttp.withMock(
              MockHttp.forExistingUCTable(tableId, dir.getAbsolutePath()),
              mockHttp -> {
                UnityCatalog uc =
                    new UnityCatalog(
                        "main", mockHttp.uri(), "", /* credentialVendingEnabled = */ false);
                uc.open();

                DeltaCatalog.TableDescriptor tableDescriptor = uc.getTable("main.default.dummy");
                assertEquals(tableId, tableDescriptor.uuid);
                assertEquals(
                    AbstractKernelTable.normalize(URI.create(dir.getAbsolutePath())),
                    tableDescriptor.tablePath);
                mockHttp.verifyCredentialRequests(0);
              });
        });
  }

  @Test
  void testGetCredentials() {
    String tableId = UUID.randomUUID().toString();
    String credentialsResponse =
        "{\"storage-credentials\":[{\"prefix\":\"s3://bucket/table\","
            + "\"operation\":\"READ_WRITE\",\"expiration-time-ms\":4102444800000,"
            + "\"config\":{\"s3.access-key-id\":\"ak\","
            + "\"s3.secret-access-key\":\"sk\",\"s3.session-token\":\"st\"}}]}";

    MockHttp.withMock(
        MockHttp.forExistingUCTable(tableId, "s3://bucket/table", credentialsResponse),
        mockHttp -> {
          UnityCatalog uc = new UnityCatalog("main", mockHttp.uri(), "");
          uc.open();

          DeltaCatalog.TableDescriptor table = uc.getTable("main.default.dummy");
          assertEquals("ak", table.getStorageProperties().get("fs.s3a.init.access.key"));
          mockHttp.verifyCredentialRequests(1);

          Map<String, String> credentials = uc.getCredentials("main.default.dummy");

          assertEquals("ak", credentials.get("fs.s3a.init.access.key"));
          mockHttp.verifyCredentialRequests(1);
        });
  }

  @Test
  void testCreateTableDescriptorIncludesStagingProperties() {
    withTempDir(
        dir -> {
          String tableName = "main.default.tbl";
          String uuid = UUID.randomUUID().toString();
          MockHttp.withMock(
              MockHttp.forNewUCTable(uuid, dir.getAbsolutePath()),
              mockHttp -> {
                UnityCatalog uc = new UnityCatalog("main", mockHttp.uri(), "");
                uc.open();

                uc.createTable(
                    tableName,
                    new StructType().add("id", IntegerType.INTEGER),
                    List.of(),
                    Map.of(),
                    descriptor -> {
                      assertEquals(uuid, descriptor.getUuid());
                      assertEquals(Map.of(), descriptor.getStorageProperties());
                      assertEquals(
                          "true",
                          descriptor.getRequiredProperties().get("delta.enableDeletionVectors"));
                    });
              });
        });
  }
}
