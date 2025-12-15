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

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/** Tests for {@link DeltaSnapshotManagerFactory}. */
class DeltaSnapshotManagerFactoryTest {

  @Test
  void testCreate_NullTablePath_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> DeltaSnapshotManagerFactory.create(null, Optional.empty(), null, new Configuration()),
        "Null tablePath should throw NullPointerException");
  }

  @Test
  void testCreate_NullCatalogTable_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> DeltaSnapshotManagerFactory.create("/tmp/test", null, null, new Configuration()),
        "Null catalogTable should throw NullPointerException");
  }

  @Test
  void testCreate_NullSpark_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () ->
            DeltaSnapshotManagerFactory.create(
                "/tmp/test", Optional.empty(), null, new Configuration()),
        "Null spark should throw NullPointerException");
  }

  @Test
  void testCreate_NullHadoopConf_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> DeltaSnapshotManagerFactory.create("/tmp/test", Optional.empty(), null, null),
        "Null hadoopConf should throw NullPointerException");
  }

  // Note: Factory behavior tests (which manager type is created) require integration test setup.
  // The following tests cannot be implemented as unit tests because the factory requires
  // a non-null SparkSession parameter:
  //
  // - testCreate_NonCatalogManagedTable_ReturnsPathBasedManager: Verify non-UC tables use PathBased
  // - testCreate_EmptyCatalogTable_ReturnsPathBasedManager: Verify empty catalogTable uses
  // PathBased
  // - testCreate_UCManagedTable_ReturnsCatalogManagedManager: Verify UC tables use CatalogManaged
  //
  // While PathBasedSnapshotManager doesn't technically need SparkSession, the factory API requires
  // it to maintain a clean, consistent interface (always available in production via SparkTable).
  // Cannot mock SparkSession effectively for these tests.
  //
  // Note: Testing CatalogManagedSnapshotManager creation requires integration tests with
  // real SparkSession and Unity Catalog configuration. This is because:
  // 1. CatalogManagedSnapshotManager constructor validates UC table and extracts metadata
  // 2. It requires configured UC catalog (spark.sql.catalog.*.uri/token)
  // 3. Unit tests cannot easily mock SparkSession's catalog manager
  // See CatalogManagedSnapshotManagerTest for integration tests.
}
