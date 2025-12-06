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

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/** Tests for {@link DeltaSnapshotManagerFactory}. */
class DeltaSnapshotManagerFactoryTest {

  @Test
  void testFromPath_NullTablePath_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> DeltaSnapshotManagerFactory.fromPath(null, new Configuration()),
        "Null tablePath should throw NullPointerException");
  }

  @Test
  void testFromPath_NullHadoopConf_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> DeltaSnapshotManagerFactory.fromPath("/tmp/test", null),
        "Null hadoopConf should throw NullPointerException");
  }

  @Test
  void testFromCatalogTable_NullCatalogTable_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> DeltaSnapshotManagerFactory.fromCatalogTable(null, null, new Configuration()),
        "Null catalogTable should throw NullPointerException");
  }
}
