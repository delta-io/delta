package io.delta.flink.table;

import io.delta.kernel.Snapshot;
import io.delta.kernel.types.StructType;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CCv2KernelTable implementation supports loading table from a catalog using UC Open API and
 * committing table to a CCv2 catalog.
 *
 * <p>TODO: CCv2KernelTable does not support creating new tables. It must refer to an existing
 * catalog table.
 */
public class CCv2KernelTable extends AbstractKernelTable {

  private static Logger LOG = LoggerFactory.getLogger(CCv2KernelTable.class);

  public static String CATALOG_ENDPOINT = "catalog.endpoint";
  public static String CATALOG_TOKEN = "catalog.token";

  private final String tableId;
  private String tableUUID;

  public CCv2KernelTable(String tableId, Map<String, String> conf) {
    super(conf);
    this.tableId = tableId;
    loadCatalogTable();
    postInit();
  }

  public CCv2KernelTable(
      String tableId, Map<String, String> conf, StructType schema, List<String> partitionColumns) {
    super(conf, schema, partitionColumns);
    this.tableId = tableId;
    loadCatalogTable();
    postInit();
  }

  protected void loadCatalogTable() {
    TablesApi tablesApi = new TablesApi(getApiClient());
    try {
      TableInfo tableInfo = tablesApi.getTable(getId());
      this.tablePath =
          normalize(URI.create(Objects.requireNonNull(tableInfo.getStorageLocation())));
      this.tableUUID = tableInfo.getTableId();
      LOG.debug("loaded table with UUID {} at {}", tableUUID, tablePath);
      refreshCredential();
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  void refreshCredential() throws ApiException {
    TemporaryCredentialsApi credentialsApi = new TemporaryCredentialsApi(getApiClient());
    TemporaryCredentials credentials =
        credentialsApi.generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential()
                .tableId(tableUUID)
                .operation(TableOperation.READ_WRITE));
    // AWS credentials
    // TODO Azure and GCP
    if (credentials.getAwsTempCredentials() != null) {
      configuration.put("fs.s3a.access.key", credentials.getAwsTempCredentials().getAccessKeyId());
      configuration.put(
          "fs.s3a.secret.key", credentials.getAwsTempCredentials().getSecretAccessKey());
      configuration.put(
          "fs.s3a.session.token", credentials.getAwsTempCredentials().getSessionToken());
    }

    // Force a refresh of engine
    engine = null;
  }

  protected transient ApiClient apiClient;

  protected ApiClient getApiClient() {
    URI parsedUri = URI.create(configuration.get(CATALOG_ENDPOINT));
    String token = configuration.get(CATALOG_TOKEN);
    if (apiClient == null) {
      apiClient =
          new ApiClient()
              .setScheme(parsedUri.getScheme())
              .setHost(parsedUri.getHost())
              .setPort(parsedUri.getPort())
              .setRequestInterceptor(request -> request.header("Authorization", "Bearer " + token));
    }
    return apiClient;
  }

  protected transient UCCatalogManagedClient ccv2Client;

  protected UCCatalogManagedClient getCcv2Client() {
    if (ccv2Client == null) {
      String endpointUri = configuration.get(CATALOG_ENDPOINT);
      String token = configuration.get(CATALOG_TOKEN);
      UCClient storageClient = new UCTokenBasedRestClient(endpointUri, token);
      ccv2Client = new UCCatalogManagedClient(storageClient);
    }
    return ccv2Client;
  }

  @Override
  public String getId() {
    return tableId;
  }

  @Override
  protected Snapshot loadLatestSnapshot() {
    // TODO refresh credentials on exception
    return getCcv2Client()
        .loadSnapshot(
            getEngine(), tableUUID, tablePath.toString(), Optional.empty(), Optional.empty());
  }
}
