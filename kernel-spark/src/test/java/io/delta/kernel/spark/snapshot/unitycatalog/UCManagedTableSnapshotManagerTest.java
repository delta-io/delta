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

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.unitycatalog.InMemoryUCClient;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link UCManagedTableSnapshotManager}.
 *
 * <p>These tests focus on constructor validation and basic instantiation. Integration tests for
 * actual functionality are in {@code UCManagedTableSnapshotManagerSuite}.
 */
public class UCManagedTableSnapshotManagerTest {

  private static final String TEST_TABLE_ID = "test_uc_table_id";
  private static final String TEST_TABLE_PATH = "/test/path/to/table";
  private static final String TEST_UC_URI = "https://uc.example.com";
  private static final String TEST_UC_TOKEN = "test_token";

  private UCTableInfo createTestTableInfo() {
    return new UCTableInfo(TEST_TABLE_ID, TEST_TABLE_PATH, TEST_UC_URI, TEST_UC_TOKEN);
  }

  private UCCatalogManagedClient createTestClient() {
    InMemoryUCClient ucClient = new InMemoryUCClient("test_metastore");
    return new UCCatalogManagedClient(ucClient);
  }

  private Engine createTestEngine() {
    return DefaultEngine.create(new Configuration());
  }

  @Test
  public void testConstructor_NullClient_ThrowsNPE() {
    UCTableInfo tableInfo = createTestTableInfo();
    Engine engine = createTestEngine();

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> new UCManagedTableSnapshotManager(null, tableInfo, engine));
    assertEquals("ucCatalogManagedClient is null", exception.getMessage());
  }

  @Test
  public void testConstructor_NullTableInfo_ThrowsNPE() {
    UCCatalogManagedClient client = createTestClient();
    Engine engine = createTestEngine();

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> new UCManagedTableSnapshotManager(client, null, engine));
    assertEquals("tableInfo is null", exception.getMessage());
  }

  @Test
  public void testConstructor_NullEngine_ThrowsNPE() {
    UCCatalogManagedClient client = createTestClient();
    UCTableInfo tableInfo = createTestTableInfo();

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> new UCManagedTableSnapshotManager(client, tableInfo, null));
    assertEquals("engine is null", exception.getMessage());
  }

  @Test
  public void testConstructor_ValidInputs_CreatesManager() {
    UCCatalogManagedClient client = createTestClient();
    UCTableInfo tableInfo = createTestTableInfo();
    Engine engine = createTestEngine();

    UCManagedTableSnapshotManager manager =
        new UCManagedTableSnapshotManager(client, tableInfo, engine);

    assertNotNull(manager);
  }
}
