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
package io.delta.kernel.spark.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Map;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat$;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTable$;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType$;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Option$;
import scala.collection.immutable.Map$;
import scala.jdk.javaapi.CollectionConverters;

/** Tests for {@link CatalogTableUtils}. */
class CatalogTableUtilsTest {

  @Test
  void catalogManagedFlagEnablesDetection() {
    // Arrange: table with catalogManaged flag set to "supported"
    CatalogTable table =
        catalogTableWithProperties(Map.of(CatalogTableUtils.FEATURE_CATALOG_MANAGED, "supported"));

    // Act & Assert
    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Should detect catalog management with flag");
    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table), "Should not detect Unity without ID");
  }

  @Test
  void previewFlagEnablesDetectionIgnoringCase() {
    // Arrange: table with preview flag set to mixed case
    CatalogTable table =
        catalogTableWithProperties(
            Map.of(CatalogTableUtils.FEATURE_CATALOG_OWNED_PREVIEW, "SuPpOrTeD"));

    // Act & Assert
    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Should detect via preview flag ignoring case");
    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table), "Should not detect Unity without ID");
  }

  @Test
  void noFlagsMeansNotManaged() {
    // Arrange: empty properties
    CatalogTable table = catalogTableWithProperties(Collections.emptyMap());

    // Act & Assert
    assertFalse(
        CatalogTableUtils.isCatalogManaged(table), "Should not detect management without flags");
    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table),
        "Should not detect Unity without ID or flags");
  }

  @Test
  void unityManagementRequiresFlagAndId() {
    // Arrange: table with both flag and UC ID
    CatalogTable table =
        catalogTableWithProperties(
            Map.of(
                CatalogTableUtils.FEATURE_CATALOG_MANAGED, "supported",
                io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
                        .UC_TABLE_ID_KEY,
                    "abc-123"));

    // Act & Assert
    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Should detect general catalog management");
    assertTrue(
        CatalogTableUtils.isUnityCatalogManagedTable(table), "Should detect Unity with ID present");
  }

  @Test
  void unityManagementFailsWithoutId() {
    // Arrange: table with flag but no UC ID
    CatalogTable table =
        catalogTableWithProperties(Map.of(CatalogTableUtils.FEATURE_CATALOG_MANAGED, "supported"));

    // Act & Assert
    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Should detect general catalog management");
    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table),
        "Should fail Unity detection without ID");
  }

  private static CatalogTable catalogTableWithProperties(Map<String, String> properties) {
    scala.collection.immutable.Map<String, String> scalaProps =
        properties.isEmpty()
            ? Map$.MODULE$.empty()
            : scala.collection.immutable.Map$.MODULE$.from(
                CollectionConverters.asScala(properties).toSeq());

    @SuppressWarnings("unchecked")
    scala.collection.immutable.Seq<String> emptySeq =
        (scala.collection.immutable.Seq<String>) scala.collection.immutable.Seq$.MODULE$.empty();

    return CatalogTable$.MODULE$.apply(
        new TableIdentifier("tbl", Option$.MODULE$.empty(), Option$.MODULE$.empty()),
        CatalogTableType$.MODULE$.MANAGED(),
        CatalogStorageFormat$.MODULE$.empty(),
        new StructType(),
        Option$.MODULE$.empty(), // provider: Option[String]
        emptySeq, // partitionColumnNames: Seq[String]
        Option$.MODULE$.empty(), // bucketSpec: Option[BucketSpec]
        "", // owner: String
        0L, // createTime: Long
        -1L, // lastAccessTime: Long
        "", // createVersion: String
        scalaProps, // properties: Map[String, String]
        Option$.MODULE$.empty(), // stats: Option[CatalogStatistics]
        Option$.MODULE$.empty(), // viewText: Option[String]
        Option$.MODULE$.empty(), // comment: Option[String]
        emptySeq, // unsupportedFeatures: Seq[String]
        false, // tracksPartitionsInCatalog: Boolean
        false, // schemaPreservesCase: Boolean
        Map$.MODULE$.empty(), // ignoredProperties: Map[String, String]
        Option$.MODULE$.empty() // viewOriginalText: Option[String]
        );
  }
}
