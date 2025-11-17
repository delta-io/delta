package io.delta.kernel.spark.unity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.spark.SparkDsv2TestBase;
import io.delta.kernel.spark.utils.CatalogTableUtils;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.CommitFailedException;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class UnityCatalogClientFactoryTest extends SparkDsv2TestBase {

  private static final String UC_CATALOG = "main";

  private final AtomicReference<String> recordedUri = new AtomicReference<>();
  private final AtomicReference<String> recordedToken = new AtomicReference<>();

  @AfterEach
  public void resetOverrides() {
    SparkSession spark = SparkDsv2TestBase.spark;
    unsetIfDefined(spark, confKey(UC_CATALOG));
    unsetIfDefined(spark, confKey(UC_CATALOG, "uri"));
    unsetIfDefined(spark, confKey(UC_CATALOG, "token"));
    unsetIfDefined(spark, confKey(UC_CATALOG, "warehouse"));
    UnityCatalogClientFactory.resetClientBuilderForTesting();
    recordedToken.set(null);
    recordedUri.set(null);
  }

  @Test
  public void createUnityCatalogClientFromConfigs() throws Exception {
    SparkSession spark = SparkDsv2TestBase.spark;
    spark.conf().set(confKey(UC_CATALOG), "io.unitycatalog.spark.UCSingleCatalog");
    spark.conf().set(confKey(UC_CATALOG, "uri"), "https://example.cloud.databricks.com");
    spark.conf().set(confKey(UC_CATALOG, "token"), "dapi123");
    spark.conf().set(confKey(UC_CATALOG, "warehouse"), "/mnt/warehouse");

    UnityCatalogClientFactory.setClientBuilderForTesting(
        (uri, token) -> {
          recordedUri.set(uri);
          recordedToken.set(token);
          return new RecordingUCClient();
        });

    CatalogTable catalogTable = buildUnityCatalogTable("table-123");
    Identifier identifier = Identifier.of(new String[] {UC_CATALOG, "default"}, "tbl");

    Optional<UnityCatalogClientFactory.UnityCatalogClient> clientOpt =
        UnityCatalogClientFactory.create(spark, identifier, catalogTable);

    assertTrue(clientOpt.isPresent(), "Expected Unity Catalog client to be created");
    UnityCatalogClientFactory.UnityCatalogClient client = clientOpt.get();
    assertEquals(UC_CATALOG, client.getCatalogName());
    assertEquals(Optional.of("/mnt/warehouse"), client.getWarehouse());
    assertEquals("https://example.cloud.databricks.com", recordedUri.get());
    assertEquals("dapi123", recordedToken.get());

    client.close();
  }

  @Test
  public void missingTokenConfigurationThrows() {
    SparkSession spark = SparkDsv2TestBase.spark;
    spark.conf().set(confKey(UC_CATALOG), "io.unitycatalog.spark.UCSingleCatalog");
    spark.conf().set(confKey(UC_CATALOG, "uri"), "https://example.cloud.databricks.com");

    CatalogTable catalogTable = buildUnityCatalogTable("table-456");
    Identifier identifier = Identifier.of(new String[] {UC_CATALOG, "default"}, "tbl");

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> UnityCatalogClientFactory.create(spark, identifier, catalogTable));

    assertTrue(
        thrown.getMessage().contains("token"),
        "Exception message should mention missing token configuration");
  }

  @Test
  public void usesSoleCatalogWhenIdentifierUnqualified() throws Exception {
    SparkSession spark = SparkDsv2TestBase.spark;
    spark.conf().set(confKey(UC_CATALOG), "io.unitycatalog.spark.UCSingleCatalog");
    spark.conf().set(confKey(UC_CATALOG, "uri"), "https://example.cloud.databricks.com");
    spark.conf().set(confKey(UC_CATALOG, "token"), "token-789");

    UnityCatalogClientFactory.setClientBuilderForTesting(
        (uri, token) -> {
          recordedUri.set(uri);
          recordedToken.set(token);
          return new RecordingUCClient();
        });

    CatalogTable catalogTable = buildUnityCatalogTable("table-789");
    Identifier identifier = Identifier.of(new String[] {"default"}, "tbl");

    Optional<UnityCatalogClientFactory.UnityCatalogClient> clientOpt =
        UnityCatalogClientFactory.create(spark, identifier, catalogTable);

    assertTrue(clientOpt.isPresent());
    assertEquals("https://example.cloud.databricks.com", recordedUri.get());
    assertEquals("token-789", recordedToken.get());
    clientOpt.get().close();
  }

  @Test
  public void returnsEmptyWhenTableNotUnityCatalogManaged() {
    SparkSession spark = SparkDsv2TestBase.spark;
    CatalogTable catalogTable =
        io.delta.kernel.spark.utils.CatalogTableTestUtils$.MODULE$.catalogTableWithProperties(
            new HashMap<>(), new HashMap<>());
    Identifier identifier = Identifier.of(new String[] {"default"}, "tbl");

    Optional<UnityCatalogClientFactory.UnityCatalogClient> clientOpt =
        UnityCatalogClientFactory.create(spark, identifier, catalogTable);

    assertFalse(clientOpt.isPresent());
  }

  private static CatalogTable buildUnityCatalogTable(String tableId) {
    Map<String, String> storageProps = new HashMap<>();
    storageProps.put(CatalogTableUtils.FEATURE_CATALOG_MANAGED, "supported");
    storageProps.put(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, tableId);
    return io.delta.kernel.spark.utils.CatalogTableTestUtils$.MODULE$.catalogTableWithProperties(
        new HashMap<>(), storageProps);
  }

  private static String confKey(String catalog) {
    return "spark.sql.catalog." + catalog;
  }

  private static String confKey(String catalog, String suffix) {
    return confKey(catalog) + "." + suffix;
  }

  private static void unsetIfDefined(SparkSession spark, String key) {
    try {
      spark.conf().unset(key);
    } catch (IllegalArgumentException ignored) {
      // Config was not set; nothing to cleanup.
    }
  }

  private static final class RecordingUCClient implements UCClient {

    @Override
    public String getMetastoreId() {
      return "test-metastore";
    }

    @Override
    public void commit(
        String tableId,
        URI tableUri,
        Optional<Commit> commit,
        Optional<Long> lastKnownBackfilledVersion,
        boolean disown,
        Optional<AbstractMetadata> newMetadata,
        Optional<AbstractProtocol> newProtocol)
        throws IOException, CommitFailedException, UCCommitCoordinatorException {
      throw new UnsupportedOperationException("Not implemented in test stub");
    }

    @Override
    public GetCommitsResponse getCommits(
        String tableId, URI tableUri, Optional<Long> startVersion, Optional<Long> endVersion)
        throws IOException, UCCommitCoordinatorException {
      throw new UnsupportedOperationException("Not implemented in test stub");
    }

    @Override
    public void close() {
      // no-op
    }
  }
}
