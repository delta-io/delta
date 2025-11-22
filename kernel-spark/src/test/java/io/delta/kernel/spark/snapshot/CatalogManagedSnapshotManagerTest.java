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
package io.delta.kernel.spark.snapshot;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.kernel.spark.utils.CatalogTableTestUtils$;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.junit.jupiter.api.Test;

/** Tests for {@link CatalogManagedSnapshotManager}. */
class CatalogManagedSnapshotManagerTest {

  @Test
  void testConstructor_NullCatalogTable_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> new CatalogManagedSnapshotManager(null, null, new Configuration()),
        "Null catalogTable should throw NullPointerException");
  }

  @Test
  void testConstructor_NullSpark_ThrowsException() {
    CatalogTable table = catalogTable(Collections.emptyMap(), Collections.emptyMap());

    assertThrows(
        NullPointerException.class,
        () -> new CatalogManagedSnapshotManager(table, null, new Configuration()),
        "Null spark should throw NullPointerException");
  }

  @Test
  void testConstructor_NullHadoopConf_ThrowsException() {
    CatalogTable table = catalogTable(Collections.emptyMap(), Collections.emptyMap());

    assertThrows(
        NullPointerException.class,
        () -> new CatalogManagedSnapshotManager(table, null, null),
        "Null hadoopConf should throw NullPointerException");
  }

  // Note: Additional constructor validation tests require integration test setup.
  // The following tests cannot be implemented as unit tests because they require
  // a real SparkSession to properly test UC table validation logic:
  //
  // - testConstructor_NonUCTable_ThrowsException: Verify non-UC table rejection
  // - testConstructor_CatalogManagedWithoutUCTableId_ThrowsException: Verify UC table ID
  // requirement
  //
  // These validations happen after null checks, so passing null SparkSession causes
  // NullPointerException before reaching the UC validation logic. Cannot mock SparkSession
  // effectively for these tests.
  //
  // Note: Full integration tests for CatalogManagedSnapshotManager require:
  // 1. Real SparkSession with Unity Catalog configured
  // 2. Unity Catalog tables with proper feature flags and table IDs
  // 3. Unity Catalog endpoint available for snapshot loading
  //
  // Such tests should be added when integration test infrastructure is available:
  // - testConstructor_ValidUCTable_Success: Happy path with valid UC table and config
  // - testLoadLatestSnapshot_Success: Test snapshot loading from UC
  // - testLoadSnapshotAt_Success: Test version-specific snapshot loading
  // - testClose_Success: Test UC client cleanup

  private static CatalogTable catalogTable(
      Map<String, String> properties, Map<String, String> storageProperties) {
    return CatalogTableTestUtils$.MODULE$.catalogTableWithProperties(properties, storageProperties);
  }
}
