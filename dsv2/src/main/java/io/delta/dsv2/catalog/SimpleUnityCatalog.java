package io.delta.dsv2.catalog;

import io.delta.dsv2.table.DeltaCcv2Table;
import io.delta.kernel.ResolvedTable;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.delta.unity.UCCatalogManagedClient;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.TableInfo;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** A simple implementation of TableCatalog for Unity Catalog. */
public class SimpleUnityCatalog implements TableCatalog {

  private String catalogName;
  private ApiClient ucApiClient;
  private UCClient ucCcClient;
  private UCCatalogManagedClient ucCatalogManagedClient;
  private TablesApi tablesApi;
  private Engine engine;

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    // Verify namespace
    if (namespace.length != 1) {
      throw new NoSuchNamespaceException(namespace);
    }

    try {
      // Get tables for the given namespace
      return tablesApi.listTables(catalogName, namespace[0], 0, null).getTables().stream()
          .map(table -> Identifier.of(namespace, table.getName()))
          .toArray(Identifier[]::new);
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        throw new NoSuchNamespaceException(namespace);
      }
      throw new RuntimeException("Failed to list tables", e);
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    if (ident.namespace().length != 1) {
      throw new NoSuchTableException(ident);
    }

    try {
      // Get the table ID from Unity Catalog
      String fullTableName =
          String.format(
              "%s.%s.%s", "managed_iceberg_bugbash_pupr", ident.namespace()[0], ident.name());
      TableInfo tableInfo = tablesApi.getTable(fullTableName);
      // Load the table using UCCatalogManagedClient
      ResolvedTable table =
          ucCatalogManagedClient.loadTable(
              engine, tableInfo.getTableId(), tableInfo.getStorageLocation(), 1);
      return new DeltaCcv2Table(table, ident, engine);
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        throw new NoSuchTableException(ident);
      }
      throw new RuntimeException("Failed to load table" + e);
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    throw new UnsupportedOperationException("Creating tables is not supported yet");
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new UnsupportedOperationException("Altering tables is not supported yet");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    if (ident.namespace().length != 1) {
      return false;
    }

    try {
      String fullTableName =
          String.format("%s.%s.%s", catalogName, ident.namespace()[0], ident.name());
      tablesApi.deleteTable(fullTableName);
      return true;
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        return false;
      }
      throw new RuntimeException("Failed to drop table", e);
    }
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("Renaming tables is not supported yet");
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;

    // Get URI and token from options
    String uri = options.get("uri");
    if (uri == null) {
      throw new IllegalArgumentException(
          String.format("uri must be specified for catalog '%s'", name));
    }

    String token = options.get("token");
    if (token == null || token.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("token must be specified for catalog '%s'", name));
    }

    try {
      // Initialize API client
      URI parsedUri = new URI(uri);
      this.ucApiClient =
          new ApiClient()
              .setScheme(parsedUri.getScheme())
              .setHost(parsedUri.getHost())
              .setPort(parsedUri.getPort());

      // Add authentication
      this.ucApiClient.setRequestInterceptor(
          request -> request.header("Authorization", "Bearer " + token));

      // Initialize UC clients
      this.ucCcClient = new UCTokenBasedRestClient(uri, token);
      this.ucCatalogManagedClient = new UCCatalogManagedClient(ucCcClient);
      this.tablesApi = new TablesApi(ucApiClient);
      Configuration conf = new Configuration();
      //        conf.set("fs.s3a.path.style.access", "true");
      conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      conf.set("fs.s3.region", "us-west-2");
      conf.set(
          "fs.s3.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
      conf.set("fs.s3.endpoint", "s3.us-west-2.amazonaws.com");

      // Initialize engine (this would typically be injected or obtained from a factory)
      this.engine = DefaultEngine.create(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize Unity Catalog client", e);
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  /** Checks if a table exists in the catalog. */
  @Override
  public boolean tableExists(Identifier ident) {
    if (ident.namespace().length != 1) {
      return false;
    }

    try {
      String fullTableName =
          String.format("%s.%s.%s", catalogName, ident.namespace()[0], ident.name());
      tablesApi.getTable(fullTableName);
      return true;
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        return false;
      }
      throw new RuntimeException("Failed to check if table exists", e);
    }
  }
}
