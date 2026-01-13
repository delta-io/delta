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

package io.sparkuctest;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Abstract base class that provides Unity Catalog server integration for Delta tests.
 *
 * <p>Automatically starts a local Unity Catalog server before tests and stops it after. To use a
 * remote server instead, set {@code UC_REMOTE=true} and configure {@code UC_URI}, {@code UC_TOKEN},
 * {@code UC_CATALOG_NAME}, {@code UC_SCHEMA_NAME}, and {@code UC_BASE_TABLE_LOCATION}.
 *
 * <p>{@code unityCatalogInfo()} is the only API for subclasses, All other methods are internal
 * implementation details.
 *
 * <pre>
 * public class MyUCTest extends UnityCatalogSupport {
 *   {@literal @}Test
 *   public void myTest() {
 *     UnityCatalogInfo ucInfo = unityCatalogInfo();
 *     String tableName = ucInfo.catalogName() + "." + ucInfo.schemaName() + ".my_table";
 *     spark.sql("CREATE TABLE " + tableName + " (id INT) USING DELTA");
 *   }
 * }
 * </pre>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class UnityCatalogSupport {

  private static final Logger LOG = Logger.getLogger(UnityCatalogSupport.class);

  protected static class UnityCatalogInfo {

    private final String serverUri;
    private final String catalogName;
    private final String serverToken;
    private final String schemaName;
    private final String baseTableLocation;

    public UnityCatalogInfo(
        String serverUri,
        String catalogName,
        String serverToken,
        String schemaName,
        String baseTableLocation) {
      this.serverUri = serverUri;
      this.catalogName = catalogName;
      this.serverToken = serverToken;
      this.schemaName = schemaName;
      this.baseTableLocation = baseTableLocation;
    }

    public String serverUri() {
      return serverUri;
    }

    public String catalogName() {
      return catalogName;
    }

    public String serverToken() {
      return serverToken;
    }

    public String schemaName() {
      return schemaName;
    }

    public String baseTableLocation() {
      return baseTableLocation;
    }

    /** Creates a configured Unity Catalog API client. */
    public ApiClient createApiClient() {
      return ApiClientBuilder.create()
          .uri(serverUri)
          .tokenProvider(
              TokenProvider.create(ImmutableMap.of("type", "static", "token", serverToken)))
          .build();
    }
  }

  public static final String UC_STATIC_TOKEN = "static-token";

  // Environment variables for configuring access to remote unity catalog server.
  public static final String UC_REMOTE = "UC_REMOTE";
  public static final String UC_URI = "UC_URI";
  public static final String UC_TOKEN = "UC_TOKEN";
  public static final String UC_CATALOG_NAME = "UC_CATALOG_NAME";
  public static final String UC_SCHEMA_NAME = "UC_SCHEMA_NAME";
  public static final String UC_BASE_TABLE_LOCATION = "UC_BASE_TABLE_LOCATION";

  private static boolean isUCRemoteConfigured() {
    String ucRemote = System.getenv(UC_REMOTE);
    return ucRemote != null && ucRemote.equalsIgnoreCase("true");
  }

  /** The Unity Catalog info instance for subclasses access */
  private UnityCatalogInfo ucInfo = null;

  /** The Unity Catalog server instance. */
  private UnityCatalogServer ucServer;

  /** The port on which the UC server is running. */
  private int ucServerPort;

  /** The temporary directory for UC server data. */
  private File ucServerDir;

  /** The temporary directory for external table location */
  private File ucBaseTableLocation = null;

  /**
   * Returns the Unity Catalog configuration for use in tests.
   *
   * <p>This is the primary method subclasses should use to access Unity Catalog connection details,
   * authentication tokens, and storage locations.
   *
   * <p><strong>Note:</strong> This is the only public API intended for subclasses. All other
   * methods are internal implementation details.
   *
   * @return the Unity Catalog configuration
   * @see UnityCatalogInfo
   */
  protected synchronized UnityCatalogInfo unityCatalogInfo() {
    Preconditions.checkNotNull(
        ucInfo,
        "No UnityCatalogInfo available, please make sure the unity catalog server is available");
    return ucInfo;
  }

  private UnityCatalogInfo remoteUnityCatalogInfo() {
    String serverUri = System.getenv(UC_URI);
    String catalogName = System.getenv(UC_CATALOG_NAME);
    String serverToken = System.getenv(UC_TOKEN);
    String schemaName = System.getenv(UC_SCHEMA_NAME);
    String baseTableLocation = System.getenv(UC_BASE_TABLE_LOCATION);
    Preconditions.checkNotNull(serverUri, "%s must be set when UC_REMOTE=true", UC_URI);
    Preconditions.checkNotNull(catalogName, "%s must be set when UC_REMOTE=true", UC_CATALOG_NAME);
    Preconditions.checkNotNull(serverToken, "%s must be set when UC_REMOTE=true", UC_TOKEN);
    Preconditions.checkNotNull(schemaName, "%s must be set when UC_REMOTE=true", UC_SCHEMA_NAME);
    Preconditions.checkNotNull(
        baseTableLocation, "%s must be set when UC_REMOTE=true", UC_BASE_TABLE_LOCATION);
    return new UnityCatalogInfo(serverUri, catalogName, serverToken, schemaName, baseTableLocation);
  }

  private UnityCatalogInfo localUnityCatalogInfo() {
    Preconditions.checkNotNull(ucServer, "Local Unity Catalog Server is not configured");
    Preconditions.checkNotNull(
        ucBaseTableLocation, "Local Unity Catalog Temp Directory is not configured");
    // For local UC, use default schema and temp directory
    return new UnityCatalogInfo(
        String.format("http://localhost:%s/", ucServerPort),
        "unity",
        UC_STATIC_TOKEN,
        "default",
        "file://" + ucBaseTableLocation.getAbsolutePath());
  }

  /** Finds an available port for the UC server. */
  private int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /**
   * Starts the Unity Catalog server before all tests. IMPORTANT: Starts the server BEFORE calling
   * other setup to ensure the server is running when SharedSparkSession creates the SparkSession.
   */
  @BeforeAll
  public void setupServer() throws Exception {
    if (isUCRemoteConfigured()) {
      setUpRemoteServer();
    } else {
      setUpLocalServer();
    }
  }

  private void setUpRemoteServer() {
    // For remote UC, log the configuration
    ucInfo = remoteUnityCatalogInfo();
    LOG.info("Using remote Unity Catalog server at " + ucInfo.serverUri());
    LOG.info("Catalog: " + ucInfo.catalogName() + ", Schema: " + ucInfo.schemaName());
    LOG.info("Base location: " + ucInfo.baseTableLocation());
    LOG.info(
        "Note: Schema '"
            + ucInfo.catalogName()
            + "."
            + ucInfo.schemaName()
            + "' must already exist in the remote UC server");
  }

  private void setUpLocalServer() throws Exception {
    // Create temporary directory for UC server data
    ucServerDir = Files.createTempDirectory("unity-catalog-test-").toFile();
    ucServerDir.deleteOnExit();

    // Create temporary directory for external tables testing.
    ucBaseTableLocation = Files.createTempDirectory("base-table-location-").toFile();
    ucBaseTableLocation.deleteOnExit();

    // Find an available port
    ucServerPort = findAvailablePort();

    // Set up server properties
    Properties serverProps = new Properties();
    serverProps.setProperty("server.env", "test");
    // Enable managed tables (experimental feature in Unity Catalog)
    serverProps.setProperty("server.managed-table.enabled", "true");
    serverProps.setProperty(
        "storage-root.tables", new File(ucServerDir, "ucroot").getAbsolutePath());

    // Start UC server with configuration
    ServerProperties initServerProperties = new ServerProperties(serverProps);

    UnityCatalogServer server =
        UnityCatalogServer.builder()
            .port(ucServerPort)
            .serverProperties(initServerProperties)
            .build();

    server.start();
    ucServer = server;

    // Poll for server readiness by checking if we can create an API client and query catalogs
    int maxRetries = 30;
    int retryDelayMs = 500;
    boolean serverReady = false;
    int retries = 0;

    ucInfo = localUnityCatalogInfo();
    while (!serverReady && retries < maxRetries) {
      try {
        CatalogsApi catalogsApi = new CatalogsApi(ucInfo.createApiClient());
        catalogsApi.listCatalogs(null, null); // This will throw if server is not ready
        serverReady = true;
      } catch (Exception e) {
        Thread.sleep(retryDelayMs);
        retries++;
      }
    }

    if (!serverReady) {
      throw new RuntimeException(
          "Unity Catalog server did not become ready after " + (maxRetries * retryDelayMs) + "ms");
    }

    // Create the catalog and default schema in the UC server
    ApiClient client = ucInfo.createApiClient();

    CatalogsApi catalogsApi = new CatalogsApi(client);
    SchemasApi schemasApi = new SchemasApi(client);

    // Create catalog
    catalogsApi.createCatalog(
        new CreateCatalog()
            .name(ucInfo.catalogName())
            .comment("Test catalog for Delta Lake integration"));

    // Create default schema
    schemasApi.createSchema(new CreateSchema().name("default").catalogName(ucInfo.catalogName()));

    LOG.info("Unity Catalog server started and ready at " + ucInfo.serverUri());
    LOG.info("Created catalog '" + ucInfo.catalogName() + "' with schema 'default'");
  }

  /** Stops the Unity Catalog server after all tests. */
  @AfterAll
  public void tearDownServer() {
    if (isUCRemoteConfigured()) {
      return;
    }

    if (ucServer != null) {
      ucServer.stop();
      LOG.info("Unity Catalog server stopped");
      ucServer = null;
    }

    // Clean up uc server temporary directory
    if (ucServerDir != null && ucServerDir.exists()) {
      deleteRecursively(ucServerDir);
    }

    // Clear up base table locations.
    if (ucBaseTableLocation != null && ucBaseTableLocation.exists()) {
      deleteRecursively(ucBaseTableLocation);
    }
  }

  /** Recursively deletes a directory and all its contents. */
  private void deleteRecursively(File file) {
    FileUtils.deleteQuietly(file);
  }
}
