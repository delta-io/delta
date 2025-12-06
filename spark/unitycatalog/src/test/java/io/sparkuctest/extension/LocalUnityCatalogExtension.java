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

package io.sparkuctest.extension;

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
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalUnityCatalogExtension implements UnityCatalogExtension {

  private static final Logger LOG = LoggerFactory.getLogger(LocalUnityCatalogExtension.class);

  private File tempDir;
  private UnityCatalogServer ucServer;
  private int ucPort;
  private String ucToken = "mock-token";

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    tempDir = Files.createTempDirectory("spark-uc-test-").toFile();
    ucServer = startUnityCatalogServer();
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    if (ucServer != null) {
      ucServer.stop();
    }

    if (tempDir != null) {
      FileUtils.deleteDirectory(tempDir);
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
  public String rootTestingDir() {
    return tempDir.getAbsolutePath();
  }

  @Override
  public Map<String, String> catalogSparkConf() {
    String catalogKey = String.format("spark.sql.catalog.%s", catalogName());
    return Map.of(
        catalogKey, "io.unitycatalog.spark.UCSingleCatalog",
        catalogKey + ".uri", catalogUri(),
        catalogKey + ".token", ucToken
    );
  }

  private UnityCatalogServer startUnityCatalogServer() throws Exception {
    // Find an available port
    ucPort = findAvailablePort();

    // Set up server properties
    Properties serverProps = new Properties();
    serverProps.setProperty("server.env", "test");

    // Start UC server with configuration
    ServerProperties initServerProperties = new ServerProperties(serverProps);

    UnityCatalogServer server = UnityCatalogServer.builder()
        .port(ucPort)
        .serverProperties(initServerProperties)
        .build();

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
    ApiClient client = new ApiClient();
    client.setScheme("http");
    client.setHost("localhost");
    client.setPort(ucPort);

    CatalogsApi catalogsApi = new CatalogsApi(client);
    SchemasApi schemasApi = new SchemasApi(client);

    // Create catalog
    catalogsApi.createCatalog(
        new CreateCatalog()
            .name(catalogName())
            .comment("Test catalog for Delta Lake integration")
    );

    // Create default schema
    schemasApi.createSchema(
        new CreateSchema()
            .name(schemaName())
            .catalogName(catalogName())
    );

    LOG.info("Unity Catalog server started and ready at {}", catalogUri());
    LOG.info("Created catalog '{}' with schema '{}'", catalogName(), schemaName());

    return server;
  }

  private int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
