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

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

/** Tests for {@link SparkConfigUtils}. */
class SparkConfigUtilsTest {

  private static SparkSession spark;

  @BeforeAll
  static void setupSpark() {
    spark =
        SparkSession.builder()
            .appName("SparkConfigUtilsTest")
            .master("local[*]")
            .config("spark.sql.catalog.test_catalog.uri", "https://uc-server.example.com")
            .config("spark.sql.catalog.test_catalog.token", "test-token-123")
            .config("spark.sql.catalog.test_catalog.warehouse", "test_warehouse")
            .config("spark.sql.catalog.another_catalog.uri", "https://another-uc.example.com")
            .config("spark.sql.catalog.another_catalog.token", "another-token")
            .config("spark.some.other.config", "not-a-catalog-config")
            .getOrCreate();
  }

  @AfterAll
  static void tearDownSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void testExtractCatalogConfig_ValidCatalog_ReturnsAllProperties() {
    Map<String, String> config = SparkConfigUtils.extractCatalogConfig(spark, "test_catalog");

    assertEquals(3, config.size(), "Should extract all test_catalog properties");
    assertEquals("https://uc-server.example.com", config.get("uri"));
    assertEquals("test-token-123", config.get("token"));
    assertEquals("test_warehouse", config.get("warehouse"));
  }

  @Test
  void testExtractCatalogConfig_DifferentCatalog_ReturnsCorrectProperties() {
    Map<String, String> config = SparkConfigUtils.extractCatalogConfig(spark, "another_catalog");

    assertEquals(2, config.size(), "Should extract only another_catalog properties");
    assertEquals("https://another-uc.example.com", config.get("uri"));
    assertEquals("another-token", config.get("token"));
    assertNull(config.get("warehouse"), "Should not include warehouse from another catalog");
  }

  @Test
  void testExtractCatalogConfig_NonExistentCatalog_ReturnsEmptyMap() {
    Map<String, String> config =
        SparkConfigUtils.extractCatalogConfig(spark, "nonexistent_catalog");

    assertTrue(config.isEmpty(), "Non-existent catalog should return empty map");
  }

  @Test
  void testExtractCatalogConfig_NullSpark_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> SparkConfigUtils.extractCatalogConfig(null, "test_catalog"),
        "Null SparkSession should throw NullPointerException");
  }

  @Test
  void testExtractCatalogConfig_NullCatalogName_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> SparkConfigUtils.extractCatalogConfig(spark, null),
        "Null catalog name should throw NullPointerException");
  }

  @Test
  void testGetRequiredConfig_ValidKey_ReturnsValue() {
    Map<String, String> config = new HashMap<>();
    config.put("uri", "https://example.com");
    config.put("token", "my-token");

    String uri = SparkConfigUtils.getRequiredConfig(config, "uri");
    assertEquals("https://example.com", uri, "Should return the correct URI value");

    String token = SparkConfigUtils.getRequiredConfig(config, "token");
    assertEquals("my-token", token, "Should return the correct token value");
  }

  @Test
  void testGetRequiredConfig_MissingKey_ThrowsException() {
    Map<String, String> config = new HashMap<>();
    config.put("uri", "https://example.com");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SparkConfigUtils.getRequiredConfig(config, "token"),
            "Missing required key should throw IllegalArgumentException");

    assertTrue(
        exception.getMessage().contains("token"),
        "Exception message should mention the missing key");
    assertTrue(
        exception.getMessage().contains("missing or empty"),
        "Exception message should indicate the key is missing");
  }

  @Test
  void testGetRequiredConfig_EmptyValue_ThrowsException() {
    Map<String, String> config = new HashMap<>();
    config.put("uri", "");
    config.put("token", "   "); // Whitespace only

    IllegalArgumentException uriException =
        assertThrows(
            IllegalArgumentException.class,
            () -> SparkConfigUtils.getRequiredConfig(config, "uri"),
            "Empty value should throw IllegalArgumentException");
    assertTrue(uriException.getMessage().contains("uri"));

    IllegalArgumentException tokenException =
        assertThrows(
            IllegalArgumentException.class,
            () -> SparkConfigUtils.getRequiredConfig(config, "token"),
            "Whitespace-only value should throw IllegalArgumentException");
    assertTrue(tokenException.getMessage().contains("token"));
  }

  @Test
  void testGetRequiredConfig_NullConfig_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> SparkConfigUtils.getRequiredConfig(null, "uri"),
        "Null config should throw NullPointerException");
  }

  @Test
  void testGetRequiredConfig_NullKey_ThrowsException() {
    Map<String, String> config = new HashMap<>();
    assertThrows(
        NullPointerException.class,
        () -> SparkConfigUtils.getRequiredConfig(config, null),
        "Null key should throw NullPointerException");
  }

  @Test
  void testGetOptionalConfig_ValidKey_ReturnsValue() {
    Map<String, String> config = new HashMap<>();
    config.put("warehouse", "my_warehouse");
    config.put("timeout", "30");

    String warehouse = SparkConfigUtils.getOptionalConfig(config, "warehouse", "default_wh");
    assertEquals("my_warehouse", warehouse, "Should return the configured value");

    String timeout = SparkConfigUtils.getOptionalConfig(config, "timeout", "60");
    assertEquals("30", timeout, "Should return the configured timeout value");
  }

  @Test
  void testGetOptionalConfig_MissingKey_ReturnsDefault() {
    Map<String, String> config = new HashMap<>();
    config.put("uri", "https://example.com");

    String warehouse = SparkConfigUtils.getOptionalConfig(config, "warehouse", "default_wh");
    assertEquals("default_wh", warehouse, "Should return default value when key is missing");

    String timeout = SparkConfigUtils.getOptionalConfig(config, "timeout", "60");
    assertEquals("60", timeout, "Should return default timeout when key is missing");
  }

  @Test
  void testGetOptionalConfig_EmptyValue_ReturnsDefault() {
    Map<String, String> config = new HashMap<>();
    config.put("warehouse", "");
    config.put("timeout", "   ");

    String warehouse = SparkConfigUtils.getOptionalConfig(config, "warehouse", "default_wh");
    assertEquals("default_wh", warehouse, "Should return default value when value is empty");

    String timeout = SparkConfigUtils.getOptionalConfig(config, "timeout", "60");
    assertEquals("60", timeout, "Should return default value when value is whitespace-only");
  }

  @Test
  void testGetOptionalConfig_NullDefault_ReturnsNull() {
    Map<String, String> config = new HashMap<>();
    config.put("uri", "https://example.com");

    String warehouse = SparkConfigUtils.getOptionalConfig(config, "warehouse", null);
    assertNull(warehouse, "Should return null default when key is missing");
  }

  @Test
  void testGetOptionalConfig_NullConfig_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> SparkConfigUtils.getOptionalConfig(null, "warehouse", "default"),
        "Null config should throw NullPointerException");
  }

  @Test
  void testGetOptionalConfig_NullKey_ThrowsException() {
    Map<String, String> config = new HashMap<>();
    assertThrows(
        NullPointerException.class,
        () -> SparkConfigUtils.getOptionalConfig(config, null, "default"),
        "Null key should throw NullPointerException");
  }
}
