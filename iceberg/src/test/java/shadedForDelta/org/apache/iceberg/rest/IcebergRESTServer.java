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

package shadedForDelta.org.apache.iceberg.rest;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import shadedForDelta.org.apache.iceberg.CatalogProperties;
import shadedForDelta.org.apache.iceberg.CatalogUtil;
import shadedForDelta.org.apache.iceberg.catalog.Catalog;
import shadedForDelta.org.apache.iceberg.jdbc.JdbcCatalog;
import shadedForDelta.org.apache.iceberg.relocated.com.google.common.collect.Maps;
import shadedForDelta.org.apache.iceberg.util.PropertyUtil;
import shadedForDelta.org.apache.iceberg.expressions.Expression;
import java.util.List;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP server for testing Iceberg REST catalog operations with server-side scan planning support.
 * Uses Jetty to serve REST catalog endpoints and extends the standard REST catalog with a /plan
 * endpoint for server-side table scan planning. This implementation is suitable for integration
 * tests and does not require external services.
 */
public class IcebergRESTServer {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTServer.class);

  public static final String REST_PORT = "rest.port";
  static final int REST_PORT_DEFAULT = 8181;

  public static final String CATALOG_NAME = "catalog.name";
  static final String CATALOG_NAME_DEFAULT = "rest_backend";

  private Server httpServer;
  private final Map<String, String> config;
  private Catalog catalog;
  private Map<String, String> catalogConfiguration;
  private IcebergRESTCatalogAdapterWithPlanSupport adapter;

  public IcebergRESTServer() {
    this.config = Maps.newHashMap();
  }

  public IcebergRESTServer(Map<String, String> config) {
    this.config = config;
  }

  private void initializeBackendCatalog() throws IOException {
    // Translate environment variables to catalog properties
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.putAll(config);

    // Fallback to a JDBCCatalog impl if one is not set
    catalogProperties.putIfAbsent(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
    catalogProperties.putIfAbsent(CatalogProperties.URI, "jdbc:sqlite::memory:");
    catalogProperties.putIfAbsent("jdbc.schema-version", "V1");

    // Configure a default location if one is not specified
    String warehouseLocation = catalogProperties.get(CatalogProperties.WAREHOUSE_LOCATION);

    if (warehouseLocation == null) {
      File tmp = java.nio.file.Files.createTempDirectory("iceberg_warehouse").toFile();
      tmp.deleteOnExit();
      warehouseLocation = new File(tmp, "iceberg_data").getAbsolutePath();
      catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

      LOG.info("No warehouse location set. Defaulting to temp location: {}", warehouseLocation);
    }

    String catalogName =
        PropertyUtil.propertyAsString(catalogProperties, CATALOG_NAME, CATALOG_NAME_DEFAULT);

    LOG.info("Creating {} catalog with properties: {}", catalogName, catalogProperties);
    this.catalog = CatalogUtil.buildIcebergCatalog(catalogName, catalogProperties, new Configuration());
    this.catalogConfiguration = catalogProperties;
  }

  public void start(boolean join) throws Exception {
    initializeBackendCatalog();

    this.adapter = new IcebergRESTCatalogAdapterWithPlanSupport(catalog);
    // Use custom servlet that supports the /plan endpoint
    RESTCatalogServlet servlet = new IcebergRESTServletWithPlanSupport(adapter);

    ServletContextHandler servletContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    ServletHolder servletHolder = new ServletHolder(servlet);
    // Serve on root path for IcebergRESTCatalogPlanningClient tests
    servletContext.addServlet(servletHolder, "/*");
    servletContext.insertHandler(new GzipHandler());

    this.httpServer =
        new Server(
            PropertyUtil.propertyAsInt(catalogConfiguration, REST_PORT, REST_PORT_DEFAULT));
    httpServer.setHandler(servletContext);
    for (Connector connector : httpServer.getConnectors()) {
      ((ServerConnector) connector).setReusePort(true);
    }
    httpServer.start();

    if (join) {
      httpServer.join();
    }
  }

  public Catalog getCatalog() {
    return catalog;
  }

  public Map<String, String> getConfiguration() {
    return catalogConfiguration;
  }

  public int getPort() {
    Connector[] connectors = httpServer.getConnectors();
    if (connectors.length > 0) {
      return ((ServerConnector) connectors[0]).getLocalPort();
    } else {
      throw new IllegalStateException("HTTP server has no connectors");
    }
  }

  /**
   * Set the catalog prefix to be returned by /v1/config endpoint.
   * Used for testing prefix-based endpoint construction in Unity Catalog metadata.
   * Delegates to the adapter which handles the actual /v1/config request interception.
   *
   * @param prefix The prefix to return in config.overrides, or null for no prefix
   */
  public void setCatalogPrefix(String prefix) {
    if (adapter != null) {
      adapter.setCatalogPrefix(prefix);
    }
  }

  /**
   * Get the filter captured from the most recent /plan request.
   * Delegates to adapter. For test verification.
   */
  public Expression getCapturedFilter() {
    return IcebergRESTCatalogAdapterWithPlanSupport.getCapturedFilter();
  }

  /**
   * Get the projection (list of column names) captured from the most recent /plan request.
   * Delegates to adapter. For test verification.
   */
  public List<String> getCapturedProjection() {
    return IcebergRESTCatalogAdapterWithPlanSupport.getCapturedProjection();
  }

  /**
   * Clear captured filter and projection. Call between tests.
   */
  public void clearCaptured() {
    IcebergRESTCatalogAdapterWithPlanSupport.clearCaptured();
  }

  public void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  public static void main(String[] args) throws Exception {
    new IcebergRESTServer().start(true);
  }
}
