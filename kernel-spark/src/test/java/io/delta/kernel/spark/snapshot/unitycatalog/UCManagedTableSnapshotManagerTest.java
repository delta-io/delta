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
package io.delta.kernel.spark.snapshot.unitycatalog;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.spark.SparkDsv2TestBase;
import io.delta.kernel.unitycatalog.InMemoryUCClient;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link UCManagedTableSnapshotManager}.
 *
 * <p>These tests verify constructor validation for UC-managed snapshot managers.
 */
public class UCManagedTableSnapshotManagerTest extends SparkDsv2TestBase {

  private static final String TEST_TABLE_ID = "test_uc_table_id";
  private static final String TEST_TABLE_PATH = "/test/path/to/table";
  private static final String TEST_UC_URI = "https://uc.example.com";
  private static final String TEST_UC_TOKEN = "test_token";

  // ==================== Constructor Tests ====================

  @Test
  public void testConstructor_NullClient_ThrowsNPE() {
    UCTableInfo tableInfo = createTestTableInfo();
    Configuration hadoopConf = new Configuration();

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> new UCManagedTableSnapshotManager(null, tableInfo, hadoopConf));

    assertEquals("ucCatalogManagedClient is null", exception.getMessage());
  }

  @Test
  public void testConstructor_NullTableInfo_ThrowsNPE() {
    InMemoryUCClient ucClient = new InMemoryUCClient("test_metastore");
    UCCatalogManagedClient catalogClient = new UCCatalogManagedClient(ucClient);
    Configuration hadoopConf = new Configuration();

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> new UCManagedTableSnapshotManager(catalogClient, null, hadoopConf));

    assertEquals("tableInfo is null", exception.getMessage());
  }

  @Test
  public void testConstructor_NullHadoopConf_ThrowsNPE() {
    InMemoryUCClient ucClient = new InMemoryUCClient("test_metastore");
    UCCatalogManagedClient catalogClient = new UCCatalogManagedClient(ucClient);
    UCTableInfo tableInfo = createTestTableInfo();

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> new UCManagedTableSnapshotManager(catalogClient, tableInfo, null));

    assertEquals("hadoopConf is null", exception.getMessage());
  }

  @Test
  public void testConstructor_ValidInputs_CreatesManager() {
    InMemoryUCClient ucClient = new InMemoryUCClient("test_metastore");
    UCCatalogManagedClient catalogClient = new UCCatalogManagedClient(ucClient);
    UCTableInfo tableInfo = createTestTableInfo();
    Configuration hadoopConf = new Configuration();

    UCManagedTableSnapshotManager manager =
        new UCManagedTableSnapshotManager(catalogClient, tableInfo, hadoopConf);

    assertNotNull(manager);
  }

  // ==================== Helper Methods ====================

  private UCTableInfo createTestTableInfo() {
    return new UCTableInfo(TEST_TABLE_ID, TEST_TABLE_PATH, TEST_UC_URI, TEST_UC_TOKEN);
  }
}
