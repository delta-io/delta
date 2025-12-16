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

package io.sparkuctest.extensions;

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
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalUCExtension implements UCExtension {

  private static final Logger LOG = LoggerFactory.getLogger(LocalUCExtension.class);

  private File testRootDir;
  private UnityCatalogServer ucServer;
  private int ucPort;
  private static final String STATIC_TOKEN = "static-token";

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    testRootDir = Files.createTempDirectory("spark-uc-test-").toFile();
    testRootDir.deleteOnExit();

    ucServer = startUnityCatalogServer();
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    if (ucServer != null) {
      ucServer.stop();
    }

    if (testRootDir != null) {
      FileUtils.deleteDirectory(testRootDir);
    }
  }

  @Override
  public String catalogName() {
    return "unity";
  }

  @Override
  public String schemaName() {
    return "default";
  }

  @Override
  public String catalogUri() {
    return "http://localhost:" + ucPort;
  }

  @Override
  public void withTempDir(TempDirCode code) throws Exception {
    Path tempDir = new Path(testRootDir.getAbsolutePath(), UUID.randomUUID().toString());
    try {
      code.run(tempDir);
    } finally {
      FileUtils.deleteDirectory(new File(tempDir.toUri().getPath()));
    }
  }

  @Override
  public Map<String, String> catalogSparkConf() {
    String catalogKey = String.format("spark.sql.catalog.%s", catalogName());
    return Map.of(
        catalogKey,
        "io.unitycatalog.spark.UCSingleCatalog",
        catalogKey + ".uri",
        catalogUri(),
        catalogKey + ".token",
        STATIC_TOKEN);
  }

  /**
   * Creates a Unity Catalog API client configured for this server.
   */
  protected ApiClient createClient() {
    ApiClient client = new ApiClient();
    client.setScheme("http");
    client.setHost("localhost");
    client.setPort(ucPort);
    return client;
  }

  /**
   * Finds an available port for the UC server.
   */
  private int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private UnityCatalogServer startUnityCatalogServer() throws Exception {
    // Find an available port
    ucPort = findAvailablePort();

    // Set up server properties
    Properties serverProps = new Properties();
    serverProps.setProperty("server.env", "test");
    // Enable managed tables (experimental feature in Unity Catalog)
    serverProps.setProperty("server.managed-table.enabled", "true");
    serverProps.setProperty("storage-root.tables",
        new File(testRootDir, "ucRoot").getAbsolutePath());

    // Start UC server with configuration
    ServerProperties initServerProperties = new ServerProperties(serverProps);

    UnityCatalogServer server =
        UnityCatalogServer.builder().port(ucPort).serverProperties(initServerProperties).build();

    server.start();

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

    // Create the catalog and default schema in the UC server
    ApiClient client = createClient();

    CatalogsApi catalogsApi = new CatalogsApi(client);
    SchemasApi schemasApi = new SchemasApi(client);

    // Create catalog
    catalogsApi.createCatalog(
        new CreateCatalog().name(catalogName()).comment("Test catalog for Delta Lake integration"));

    // Create default schema
    schemasApi.createSchema(new CreateSchema().name(schemaName()).catalogName(catalogName()));

    LOG.info("Unity Catalog server started and ready at {}", catalogUri());
    LOG.info("Created catalog '{}' with schema '{}'", catalogName(), schemaName());

    return server;
  }
}
