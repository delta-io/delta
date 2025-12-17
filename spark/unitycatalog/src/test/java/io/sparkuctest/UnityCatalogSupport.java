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
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.SchemasApi;
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
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Abstract base class that provides Unity Catalog server integration for Delta tests.
 *
 * <p>This class automatically: - Starts a local Unity Catalog server before all tests - Configures
 * Spark to connect to the UC server - Stops the server and cleans up after all tests
 *
 * <p>The UC server runs with Unity Catalog dependencies to provide catalog functionality for
 * integration testing.
 *
 * <p>Usage:
 *
 * <pre>
 * public class MyUCTest extends UnityCatalogSupport {
 *
 *   {@literal @}Override
 *   protected SparkConf getSparkConf() {
 *     SparkConf conf = new SparkConf();
 *     // ... configure spark ...
 *     return configureSparkWithUnityCatalog(conf);
 *   }
 *
 *   {@literal @}Test
 *   public void myTest() {
 *     // Use getCatalogName() to reference the catalog
 *     getSparkSession().sql("CREATE TABLE " + getCatalogName() + ".default.test_table ...");
 *   }
 * }
 * </pre>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class UnityCatalogSupport {

  protected static class UcCatalogInfo {

    private final String serverUri;
    private final String catalogName;
    private final String serverToken;
    private final String schemaName;
    private final String baseLocation;

    public UcCatalogInfo(
        String serverUri,
        String catalogName,
        String serverToken,
        String schemaName,
        String baseLocation) {
      this.serverUri = serverUri;
      this.catalogName = catalogName;
      this.serverToken = serverToken;
      this.schemaName = schemaName;
      this.baseLocation = baseLocation;
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

    public String baseLocation() {
      return baseLocation;
    }
  }

  public static final String UC_STATIC_TOKEN = "static-token";

  public static final String UC_REMOTE_ENABLED = "UC_REMOTE_ENABLED";
  public static final String UC_URI = "UC_URI";
  public static final String UC_TOKEN = "UC_TOKEN";
  public static final String UC_CATALOG_NAME = "UC_CATALOG_NAME";
  public static final String UC_SCHEMA_NAME = "UC_SCHEMA_NAME";
  public static final String UC_BASE_LOCATION = "UC_BASE_LOCATION";

  private static boolean isUCRemoteEnabled() {
    String ucRemoteEnabled = System.getenv(UC_REMOTE_ENABLED);
    return ucRemoteEnabled != null && ucRemoteEnabled.equalsIgnoreCase("true");
  }

  protected UcCatalogInfo catalogInfo() throws IOException {
    if (isUCRemoteEnabled()) {
      String serverUri = System.getenv(UC_URI);
      String catalogName = System.getenv(UC_CATALOG_NAME);
      String serverToken = System.getenv(UC_TOKEN);
      String schemaName = System.getenv(UC_SCHEMA_NAME);
      String baseLocation = System.getenv(UC_BASE_LOCATION);
      Preconditions.checkNotNull(serverUri, "UC_URI must be set when UC_REMOTE_ENABLED=true");
      Preconditions.checkNotNull(
          catalogName, "UC_CATALOG_NAME must be set when UC_REMOTE_ENABLED=true");
      Preconditions.checkNotNull(serverToken, "UC_TOKEN must be set when UC_REMOTE_ENABLED=true");
      Preconditions.checkNotNull(
          schemaName, "UC_SCHEMA_NAME must be set when UC_REMOTE_ENABLED=true");
      Preconditions.checkNotNull(
          baseLocation, "UC_BASE_LOCATION must be set when UC_REMOTE_ENABLED=true");
      return new UcCatalogInfo(serverUri, catalogName, serverToken, schemaName, baseLocation);
    } else {
      Preconditions.checkNotNull(ucServer, "Local Unity Catalog Server is not configured");
      // For local UC, use default schema and temp directory
      return new UcCatalogInfo(
          String.format("http://localhost:" + ucPort),
          "unity",
          UC_STATIC_TOKEN,
          "default",
          // TODO: Remember to clean this directory.
          Files.createTempDirectory("spark-test-").toString()); // null baseLocation for local mode
    }
  }

  private static final Logger LOG = Logger.getLogger(UnityCatalogSupport.class);

  /** The Unity Catalog server instance. */
  private UnityCatalogServer ucServer;

  /** The port on which the UC server is running. */
  private int ucPort;

  /** The temporary directory for UC server data. */
  private File ucTempDir;

  /** Creates a Unity Catalog API client configured for this server. */
  private ApiClient createClient() {
    ApiClient client = new ApiClient();
    client.setScheme("http");
    client.setHost("localhost");
    client.setPort(ucPort);
    return client;
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
    if (isUCRemoteEnabled()) {
      // For remote UC, log the configuration
      UcCatalogInfo ucCatalog = catalogInfo();
      LOG.info("Using remote Unity Catalog server at " + ucCatalog.serverUri());
      LOG.info("Catalog: " + ucCatalog.catalogName() + ", Schema: " + ucCatalog.schemaName());
      LOG.info("Base location: " + ucCatalog.baseLocation());
      LOG.info(
          "Note: Schema '"
              + ucCatalog.catalogName()
              + "."
              + ucCatalog.schemaName()
              + "' must already exist in the remote UC server");
      return;
    }

    // Create temporary directory for UC server data
    ucTempDir = Files.createTempDirectory("unity-catalog-test-").toFile();
    ucTempDir.deleteOnExit();

    // Find an available port
    ucPort = findAvailablePort();

    // Set up server properties
    Properties serverProps = new Properties();
    serverProps.setProperty("server.env", "test");
    // Enable managed tables (experimental feature in Unity Catalog)
    serverProps.setProperty("server.managed-table.enabled", "true");
    serverProps.setProperty("storage-root.tables", new File(ucTempDir, "ucroot").getAbsolutePath());

    // Start UC server with configuration
    ServerProperties initServerProperties = new ServerProperties(serverProps);

    UnityCatalogServer server =
        UnityCatalogServer.builder().port(ucPort).serverProperties(initServerProperties).build();

    server.start();
    ucServer = server;

    // Poll for server readiness by checking if we can create an API client and query catalogs
    int maxRetries = 30;
    int retryDelayMs = 500;
    boolean serverReady = false;
    int retries = 0;

    while (!serverReady && retries < maxRetries) {
      try {
        ApiClient testClient = new ApiClient();
        testClient.setScheme("http");
        testClient.setHost("localhost");
        testClient.setPort(ucPort);
        CatalogsApi catalogsApi = new CatalogsApi(testClient);
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

    UcCatalogInfo ucCatalog = catalogInfo();

    // Create the catalog and default schema in the UC server
    ApiClient client = createClient();

    CatalogsApi catalogsApi = new CatalogsApi(client);
    SchemasApi schemasApi = new SchemasApi(client);

    // Create catalog
    catalogsApi.createCatalog(
        new CreateCatalog()
            .name(ucCatalog.catalogName())
            .comment("Test catalog for Delta Lake integration"));

    // Create default schema
    schemasApi.createSchema(
        new CreateSchema().name("default").catalogName(ucCatalog.catalogName()));

    LOG.info("Unity Catalog server started and ready at " + ucCatalog.serverUri());
    LOG.info("Created catalog '" + ucCatalog.catalogName() + "' with schema 'default'");
  }

  /** Stops the Unity Catalog server after all tests. */
  @AfterAll
  public void tearDownServer() {
    if (isUCRemoteEnabled()) {
      return;
    }

    if (ucServer != null) {
      ucServer.stop();
      LOG.info("Unity Catalog server stopped");
      ucServer = null;
    }

    // Clean up temporary directory
    if (ucTempDir != null && ucTempDir.exists()) {
      deleteRecursively(ucTempDir);
    }
  }

  /**
   * Configures a SparkConf with Unity Catalog settings.
   *
   * <p>This method should be called in the test's sparkConf override:
   *
   * <pre>
   * {@literal @}Override
   * protected SparkConf sparkConf() {
   *   return configureSparkWithUnityCatalog(super.sparkConf());
   * }
   * </pre>
   *
   * @param conf The base SparkConf to configure
   * @return The configured SparkConf with Unity Catalog settings
   */
  protected SparkConf configureSparkWithUnityCatalog(SparkConf conf) throws IOException {
    UcCatalogInfo ucCatalog = catalogInfo();
    String catalogName = ucCatalog.catalogName();
    conf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    // TODO: REMOVE this, set it temporary for run the remote UC integration tests successfully.
    conf.set("spark.databricks.delta.catalog.update.enabled", "true");
    return conf.set("spark.sql.catalog." + catalogName, "io.unitycatalog.spark.UCSingleCatalog")
        .set("spark.sql.catalog." + catalogName + ".uri", ucCatalog.serverUri())
        .set("spark.sql.catalog." + catalogName + ".token", ucCatalog.serverToken());
  }

  /** Recursively deletes a directory and all its contents. */
  private void deleteRecursively(File file) {
    FileUtils.deleteQuietly(file);
  }
}
