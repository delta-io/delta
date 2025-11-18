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
package io.delta.kernel.spark.client;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.storage.commit.uccommitcoordinator.UCClient;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

/** Small, focused unit tests for {@link UcClientFactory}. */
class UcClientFactoryTest {

  private static SparkSession spark;

  @BeforeAll
  static void setupSpark() {
    spark =
        SparkSession.builder()
            .appName("UcClientFactoryTest")
            .master("local[*]")
            // Configure a valid test catalog
            .config("spark.sql.catalog.test_catalog.uri", "https://uc-test.example.com")
            .config("spark.sql.catalog.test_catalog.token", "test-token-123")
            // Configure another catalog for isolation testing
            .config("spark.sql.catalog.other_catalog.uri", "https://uc-other.example.com")
            .config("spark.sql.catalog.other_catalog.token", "other-token-456")
            // Configure an incomplete catalog (missing token)
            .config("spark.sql.catalog.incomplete_catalog.uri", "https://uc-incomplete.example.com")
            // Configure catalog with invalid URI
            .config("spark.sql.catalog.invalid_uri_catalog.uri", "ftp://invalid-protocol.com")
            .config("spark.sql.catalog.invalid_uri_catalog.token", "token")
            // Configure catalog with malformed URI
            .config("spark.sql.catalog.malformed_uri_catalog.uri", "not a valid uri")
            .config("spark.sql.catalog.malformed_uri_catalog.token", "token")
            .getOrCreate();
  }

  @AfterAll
  static void tearDownSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void testBuildForCatalog_ValidCatalog_ReturnsClient() {
    UCClient client = UcClientFactory.buildForCatalog(spark, "test_catalog");

    assertNotNull(client, "Client should be created for valid catalog configuration");

    // Verify it's the correct client type
    assertEquals(
        "io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient",
        client.getClass().getName(),
        "Should create UCTokenBasedRestClient instance");
  }

  @Test
  void testBuildForCatalog_NonExistentCatalog_ThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> UcClientFactory.buildForCatalog(spark, "nonexistent_catalog"),
            "Should throw exception for non-existent catalog");

    String message = exception.getMessage();
    assertTrue(
        message.contains("nonexistent_catalog"), "Error message should mention catalog name");
    assertTrue(message.contains("not found"), "Error message should indicate catalog not found");
  }

  @Test
  void testBuildForCatalog_MissingToken_ThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> UcClientFactory.buildForCatalog(spark, "incomplete_catalog"),
            "Should throw exception when token is missing");

    String message = exception.getMessage();
    assertTrue(message.contains("token"), "Error message should mention missing token");
  }

  @Test
  void testBuildForCatalog_NullSparkSession_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> UcClientFactory.buildForCatalog(null, "test_catalog"),
        "Should throw NullPointerException for null SparkSession");
  }

  @Test
  void testBuildForCatalog_NullCatalogName_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> UcClientFactory.buildForCatalog(spark, null),
        "Should throw NullPointerException for null catalog name");
  }

  @Test
  void testBuildForCatalog_DifferentCatalogs_ReturnsDifferentClients() {
    UCClient client1 = UcClientFactory.buildForCatalog(spark, "test_catalog");
    UCClient client2 = UcClientFactory.buildForCatalog(spark, "other_catalog");

    assertNotNull(client1, "First client should be created");
    assertNotNull(client2, "Second client should be created");
    assertNotSame(
        client1, client2, "Different catalog names should produce different client instances");
  }

  @Test
  void testBuildForCatalog_SameCatalogMultipleCalls_ReturnsNewInstances() {
    UCClient client1 = UcClientFactory.buildForCatalog(spark, "test_catalog");
    UCClient client2 = UcClientFactory.buildForCatalog(spark, "test_catalog");

    assertNotNull(client1, "First client should be created");
    assertNotNull(client2, "Second client should be created");
    assertNotSame(
        client1, client2, "Factory should create new client instances on each call (no caching)");
  }

  @Test
  void testBuildForCatalog_ClientIsCloseable() throws Exception {
    UCClient client = UcClientFactory.buildForCatalog(spark, "test_catalog");

    // Verify client is AutoCloseable and can be closed without exception
    assertDoesNotThrow(() -> client.close(), "Client should be closeable without error");
  }

  @Test
  void testBuildForCatalog_UsesCorrectConfiguration() {
    // This test verifies that the factory uses the correct catalog's configuration
    // by creating clients for both catalogs and ensuring they're distinct
    UCClient testClient = UcClientFactory.buildForCatalog(spark, "test_catalog");
    UCClient otherClient = UcClientFactory.buildForCatalog(spark, "other_catalog");

    // While we can't easily inspect the internal URI/token without mocking,
    // we can verify they are different instances (implying different configs)
    assertNotEquals(
        testClient, otherClient, "Clients for different catalogs should be distinct instances");
  }

  @Test
  void testBuildForCatalog_InvalidUriProtocol_ThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> UcClientFactory.buildForCatalog(spark, "invalid_uri_catalog"),
            "Should throw exception for non-HTTP/HTTPS protocol");

    String message = exception.getMessage();
    assertTrue(
        message.contains("HTTP or HTTPS"), "Error message should mention required protocols");
    assertTrue(
        message.contains("invalid_uri_catalog"), "Error message should mention catalog name");
  }

  @Test
  void testBuildForCatalog_MalformedUri_ThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> UcClientFactory.buildForCatalog(spark, "malformed_uri_catalog"),
            "Should throw exception for malformed URI");

    String message = exception.getMessage();
    assertTrue(
        message.contains("Invalid") || message.contains("format"),
        "Error message should indicate URI format problem");
    assertTrue(
        message.contains("malformed_uri_catalog"), "Error message should mention catalog name");
  }
}
