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
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
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
  private String warehouseName;
  private ApiClient ucApiClient;
  private UCClient ucCcClient;
  private UCCatalogManagedClient ucCatalogManagedClient;
  private TablesApi tablesApi;
  private TemporaryCredentialsApi temporaryCredentialsApi;

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
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    if (ident.namespace().length != 1) {
      throw new NoSuchTableException(ident);
    }

    try {
      // Get the table ID from Unity Catalog
      String fullTableName =
          String.format("%s.%s.%s", warehouseName, ident.namespace()[0], ident.name());
      TableInfo tableInfo = tablesApi.getTable(fullTableName);

      // Generate temporary credentials for the table
      TemporaryCredentials temporaryCredentials =
          generateTemporaryTableCredentials(tableInfo.getTableId());

      // Extract credentials
      String accessKey = temporaryCredentials.getAwsTempCredentials().getAccessKeyId();
      String secretKey = temporaryCredentials.getAwsTempCredentials().getSecretAccessKey();
      String sessionToken = temporaryCredentials.getAwsTempCredentials().getSessionToken();

      // Create a new Engine instance with the proper credentials
      Engine engine = createEngineWithCredentials(temporaryCredentials);

      // Load the table using UCCatalogManagedClient
      ResolvedTable table =
          ucCatalogManagedClient.loadTable(
              engine, tableInfo.getTableId(), tableInfo.getStorageLocation(), Optional.empty());

      return new DeltaCcv2Table(table, ident, engine, accessKey, secretKey, sessionToken);
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        throw new NoSuchTableException(ident);
      }
      throw new RuntimeException("Failed to load table: " + e.getMessage(), e);
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
          String.format("%s.%s.%s", warehouseName, ident.namespace()[0], ident.name());
      TableInfo tableInfo = tablesApi.getTable(fullTableName);

      // Generate temporary credentials for the table
      TemporaryCredentials temporaryCredentials =
          generateTemporaryTableCredentials(tableInfo.getTableId());

      // Extract credentials
      String accessKey = temporaryCredentials.getAwsTempCredentials().getAccessKeyId();
      String secretKey = temporaryCredentials.getAwsTempCredentials().getSecretAccessKey();
      String sessionToken = temporaryCredentials.getAwsTempCredentials().getSessionToken();

      // Create a new Engine instance with the proper credentials
      Engine engine = createEngineWithCredentials(temporaryCredentials);

      // Load the table using UCCatalogManagedClient
      ResolvedTable table =
          ucCatalogManagedClient.loadTable(
              engine, tableInfo.getTableId(), tableInfo.getStorageLocation(), Optional.empty());

      return new DeltaCcv2Table(table, ident, engine, accessKey, secretKey, sessionToken);
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        throw new NoSuchTableException(ident);
      }
      throw new RuntimeException("Failed to load table: " + e.getMessage(), e);
    }
  }

  /** Generates temporary credentials for accessing a table. */
  private TemporaryCredentials generateTemporaryTableCredentials(String tableId) {
    try {
      // Try READ_WRITE first, fall back to READ if necessary
      try {
        return temporaryCredentialsApi.generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential()
                .tableId(tableId)
                .operation(TableOperation.READ_WRITE));
      } catch (ApiException e) {
        return temporaryCredentialsApi.generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential().tableId(tableId).operation(TableOperation.READ));
      }
    } catch (ApiException e) {
      throw new RuntimeException("Failed to generate temporary credentials", e);
    }
  }

  /** Creates a new Engine instance with credentials configured for the given storage location. */
  private Engine createEngineWithCredentials(TemporaryCredentials credentials) {
    Configuration conf = new Configuration();

    // Set up base configuration
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    // S3 credentials
    conf.set("fs.s3a.access.key", credentials.getAwsTempCredentials().getAccessKeyId());
    conf.set("fs.s3a.secret.key", credentials.getAwsTempCredentials().getSecretAccessKey());
    conf.set("fs.s3a.session.token", credentials.getAwsTempCredentials().getSessionToken());
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3.impl.disable.cache", "true");
    conf.set("fs.s3a.impl.disable.cache", "true");
    // Create and return a new Engine instance with these credentials
    return DefaultEngine.create(conf);
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
          String.format("%s.%s.%s", warehouseName, ident.namespace()[0], ident.name());
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

    this.warehouseName = options.get("warehouse");

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
      this.temporaryCredentialsApi = new TemporaryCredentialsApi(ucApiClient);
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
          String.format("%s.%s.%s", warehouseName, ident.namespace()[0], ident.name());
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
