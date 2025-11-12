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
    CatalogTable table =
        catalogTableWithProperties(Map.of(CatalogTableUtils.FEATURE_CATALOG_MANAGED, "supported"));

    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Should detect catalog management with flag");
    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table), "Should not detect Unity without ID");
  }

  @Test
  void previewFlagEnablesDetectionIgnoringCase() {
    CatalogTable table =
        catalogTableWithProperties(
            Map.of(CatalogTableUtils.FEATURE_CATALOG_OWNED_PREVIEW, "SuPpOrTeD"));

    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Should detect via preview flag ignoring case");
    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table), "Should not detect Unity without ID");
  }

  @Test
  void noFlagsMeansNotManaged() {
    CatalogTable table = catalogTableWithProperties(Collections.emptyMap());

    assertFalse(
        CatalogTableUtils.isCatalogManaged(table), "Should not detect management without flags");
    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table),
        "Should not detect Unity without ID or flags");
  }

  @Test
  void unityManagementRequiresFlagAndId() {
    CatalogTable table =
        catalogTableWithProperties(
            Map.of(
                CatalogTableUtils.FEATURE_CATALOG_MANAGED, "supported",
                io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
                        .UC_TABLE_ID_KEY,
                    "abc-123"));

    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Should detect general catalog management");
    assertTrue(
        CatalogTableUtils.isUnityCatalogManagedTable(table), "Should detect Unity with ID present");
  }

  @Test
  void unityManagementFailsWithoutId() {
    CatalogTable table =
        catalogTableWithProperties(Map.of(CatalogTableUtils.FEATURE_CATALOG_MANAGED, "supported"));

    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Should detect general catalog management");
    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table),
        "Should fail Unity detection without ID");
  }

  /**
   * Creates a CatalogTable with the given properties. This is a helper method to create a
   * CatalogTable for testing purposes - see interface {@link CatalogTable} for more details.
   *
   * @param properties the properties to set on the CatalogTable
   * @return a CatalogTable with the given properties
   */
  private static CatalogTable catalogTableWithProperties(Map<String, String> properties) {
    scala.collection.immutable.Map<String, String> scalaProps =
        properties.isEmpty()
            ? Map$.MODULE$.empty()
            : scala.collection.immutable.Map$.MODULE$.from(
                CollectionConverters.asScala(properties).toSeq());

    return CatalogTable$.MODULE$.apply(
        new TableIdentifier("tbl", Option$.MODULE$.empty(), Option$.MODULE$.empty()),
        CatalogTableType$.MODULE$.MANAGED(),
        CatalogStorageFormat$.MODULE$.empty(),
        new StructType(),
        scalaNone(), // provider: Option[String]
        scalaEmptySeq(), // partitionColumnNames: Seq[String]
        scalaNone(), // bucketSpec: Option[BucketSpec]
        "", // owner: String
        0L, // createTime: Long
        -1L, // lastAccessTime: Long
        "", // createVersion: String
        scalaProps, // properties: Map[String, String]
        scalaNone(), // stats: Option[CatalogStatistics]
        scalaNone(), // viewText: Option[String]
        scalaNone(), // comment: Option[String]
        scalaEmptySeq(), // unsupportedFeatures: Seq[String]
        false, // tracksPartitionsInCatalog: Boolean
        false, // schemaPreservesCase: Boolean
        scalaEmptyMap(), // ignoredProperties: Map[String, String]
        scalaNone() // viewOriginalText: Option[String]
        );
  }

  @SuppressWarnings("unchecked")
  private static <T> scala.Option<T> scalaNone() {
    return (scala.Option<T>) Option$.MODULE$.empty();
  }

  @SuppressWarnings("unchecked")
  private static <T> scala.collection.immutable.Seq<T> scalaEmptySeq() {
    return (scala.collection.immutable.Seq<T>) scala.collection.immutable.Seq$.MODULE$.empty();
  }

  @SuppressWarnings("unchecked")
  private static <K, V> scala.collection.immutable.Map<K, V> scalaEmptyMap() {
    return (scala.collection.immutable.Map<K, V>) Map$.MODULE$.empty();
  }
}
