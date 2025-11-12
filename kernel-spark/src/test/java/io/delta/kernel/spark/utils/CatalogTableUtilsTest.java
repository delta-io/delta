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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat$;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTable$;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType$;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Option;
import scala.Option$;
import scala.collection.immutable.Map$;
import scala.jdk.javaapi.CollectionConverters;

/** Tests for {@link CatalogTableUtils}. */
public class CatalogTableUtilsTest {

  @ParameterizedTest(name = "{0}")
  @MethodSource("catalogManagedMapCases")
  void mapBasedDetection(String name, Map<String, String> properties, boolean expected) {
    assertEquals(expected, CatalogTableUtils.isCatalogManagedTable(properties));
  }

  static Stream<Arguments> catalogManagedMapCases() {
    return Stream.of(
        Arguments.of("null map", null, false),
        Arguments.of("empty map", Collections.emptyMap(), false),
        Arguments.of(
            "table id present",
            javaMap(CatalogTableUtils.UNITY_CATALOG_TABLE_ID_PROP, "123"),
            true),
        Arguments.of(
            "unity catalog prefix fallback", javaMap("delta.unityCatalog.random", "enabled"), true),
        Arguments.of(
            "blank table id still catalog managed",
            javaMap(CatalogTableUtils.UNITY_CATALOG_TABLE_ID_PROP, "   "),
            true),
        Arguments.of("irrelevant properties only", javaMap("delta.feature.other", "true"), false));
  }

  @Test
  void catalogTableOverloadMatchesMapDetection() {
    Map<String, String> managed = javaMap(CatalogTableUtils.UNITY_CATALOG_TABLE_ID_PROP, "abc-123");
    Map<String, String> unmanaged = Collections.emptyMap();

    assertTrue(CatalogTableUtils.isCatalogManagedTable(catalogTableWithProperties(managed)));
    assertFalse(CatalogTableUtils.isCatalogManagedTable(catalogTableWithProperties(unmanaged)));
  }

  @ParameterizedTest(name = "feature {0} enables CCv2 when catalog managed")
  @ValueSource(
      strings = {
        CatalogTableUtils.FEATURE_CATALOG_OWNED,
        CatalogTableUtils.FEATURE_CATALOG_OWNED_PREVIEW
      })
  void isCCv2TableRecognisesSupportedFeatures(String featureKey) {
    CatalogTable table =
        catalogTableWithProperties(
            javaMap(
                CatalogTableUtils.UNITY_CATALOG_TABLE_ID_PROP,
                "table-id",
                featureKey,
                " supported "));

    assertTrue(CatalogTableUtils.isCatalogManagedTable(table));
    assertTrue(CatalogTableUtils.isCCv2Table(table));
  }

  @Test
  void isCCv2TableRequiresFeatureFlag() {
    CatalogTable table =
        catalogTableWithProperties(
            javaMap(CatalogTableUtils.UNITY_CATALOG_TABLE_ID_PROP, "table-id"));

    assertTrue(CatalogTableUtils.isCatalogManagedTable(table));
    assertFalse(CatalogTableUtils.isCCv2Table(table));
  }

  @Test
  void isCCv2TableRejectsUnsupportedValues() {
    CatalogTable table =
        catalogTableWithProperties(
            javaMap(
                CatalogTableUtils.UNITY_CATALOG_TABLE_ID_PROP,
                "table-id",
                CatalogTableUtils.FEATURE_CATALOG_OWNED,
                "disabled"));

    assertTrue(CatalogTableUtils.isCatalogManagedTable(table));
    assertFalse(CatalogTableUtils.isCCv2Table(table));
  }

  @Test
  void isCCv2TableIgnoresFeatureWithoutCatalogManagement() {
    CatalogTable table =
        catalogTableWithProperties(javaMap(CatalogTableUtils.FEATURE_CATALOG_OWNED, "supported"));

    assertFalse(CatalogTableUtils.isCatalogManagedTable(table));
    assertFalse(CatalogTableUtils.isCCv2Table(table));
  }

  private static Map<String, String> javaMap(String... keyValues) {
    if (keyValues.length % 2 != 0) {
      throw new IllegalArgumentException("keyValues length must be even");
    }
    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put(keyValues[i], keyValues[i + 1]);
    }
    return map;
  }

  private static CatalogTable catalogTableWithProperties(Map<String, String> properties) {
    Option<String> none = Option$.MODULE$.empty();
    TableIdentifier identifier = new TableIdentifier("test_table", none, none);
    CatalogStorageFormat storage = CatalogStorageFormat$.MODULE$.empty();

    scala.collection.immutable.Map<String, String> scalaProps =
        properties == null || properties.isEmpty()
            ? Map$.MODULE$.<String, String>empty()
            : scala.collection.immutable.Map$.MODULE$.<String, String>from(
                CollectionConverters.asScala(properties).toSeq());

    return CatalogTable$.MODULE$.apply(
        identifier,
        CatalogTableType$.MODULE$.MANAGED(),
        storage,
        new StructType(),
        CatalogTable$.MODULE$.apply$default$5(),
        CatalogTable$.MODULE$.apply$default$6(),
        CatalogTable$.MODULE$.apply$default$7(),
        CatalogTable$.MODULE$.apply$default$8(),
        CatalogTable$.MODULE$.apply$default$9(),
        CatalogTable$.MODULE$.apply$default$10(),
        CatalogTable$.MODULE$.apply$default$11(),
        scalaProps,
        CatalogTable$.MODULE$.apply$default$13(),
        CatalogTable$.MODULE$.apply$default$14(),
        CatalogTable$.MODULE$.apply$default$15(),
        CatalogTable$.MODULE$.apply$default$16(),
        CatalogTable$.MODULE$.apply$default$17(),
        CatalogTable$.MODULE$.apply$default$18(),
        CatalogTable$.MODULE$.apply$default$19(),
        CatalogTable$.MODULE$.apply$default$20());
  }
}
