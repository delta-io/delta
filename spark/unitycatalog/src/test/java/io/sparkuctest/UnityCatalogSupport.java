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
import io.sparkuctest.mock.MockOAuthBroker;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.VersionUtils;
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
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Abstract base class that provides Unity Catalog server integration for Delta tests.
 *
 * <p>Automatically starts a local Unity Catalog server before tests and stops it after. To use a
 * remote server instead, set {@code UC_REMOTE=true} and configure {@code UC_URI}, {@code
 * UC_CATALOG_NAME}, {@code UC_SCHEMA_NAME}, and {@code UC_BASE_TABLE_LOCATION}. Remote auth is
 * selected by which env vars are set: set {@code UC_OAUTH_URI}, {@code UC_OAUTH_CLIENT_ID}, and
 * {@code UC_OAUTH_CLIENT_SECRET} for client-credentials OAuth, or {@code UC_TOKEN} for a static
 * bearer.
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

  @Getter
  @Accessors(fluent = true)
  @AllArgsConstructor
  protected static class UnityCatalogInfo {

    private final String serverUri;
    private final String catalogName;
    private final String serverToken;
    private final String schemaName;
    private final String baseTableLocation;
    // OAuth client-credentials config; null when the test authenticates with a static token.
    private final String oauthTokenUri;
    private final String oauthClientId;
    private final String oauthClientSecret;

    /**
     * Creates a configured Unity Catalog API client for test scaffolding (catalog/schema setup and
     * assertions). Uses OAuth client-credentials when OAuth is configured, otherwise the static
     * token, so the scaffolding authenticates the same way as the connector under test.
     */
    public ApiClient createApiClient() {
      Map<String, String> authConfig =
          oauthTokenUri != null
              ? Map.of(
                  "type", "oauth",
                  "oauth.uri", oauthTokenUri,
                  "oauth.clientId", oauthClientId,
                  "oauth.clientSecret", oauthClientSecret)
              : Map.of("type", "static", "token", serverToken);
      return ApiClientBuilder.create()
          .uri(serverUri)
          .tokenProvider(TokenProvider.create(authConfig))
          .build();
    }
  }

  public static final String UC_STATIC_TOKEN = "static-token";

  /** Authentication mode for the catalog under test. */
  public enum AuthMode {
    STATIC,
    OAUTH
  }

  /** Client-credentials identity the local mock OAuth broker accepts. */
  public static final String MOCK_OAUTH_CLIENT_ID = "test-client-id";

  public static final String MOCK_OAUTH_CLIENT_SECRET = "test-client-secret";

  /** The fake S3 bucket name used for local integration tests. */
  static final String FAKE_S3_BUCKET = "fakeS3Bucket";

  // Environment variables for configuring access to remote unity catalog server.
  public static final String UC_REMOTE = "UC_REMOTE";
  public static final String UC_URI = "UC_URI";
  public static final String UC_TOKEN = "UC_TOKEN";
  public static final String UC_CATALOG_NAME = "UC_CATALOG_NAME";
  public static final String UC_SCHEMA_NAME = "UC_SCHEMA_NAME";
  public static final String UC_BASE_TABLE_LOCATION = "UC_BASE_TABLE_LOCATION";

  // OAuth client-credentials config for a remote server (used when UC_REMOTE=true and
  // UC_OAUTH_URI is set); point UC_OAUTH_URI at the real token endpoint (e.g. an OIDC provider).
  public static final String UC_OAUTH_URI = "UC_OAUTH_URI";
  public static final String UC_OAUTH_CLIENT_ID = "UC_OAUTH_CLIENT_ID";
  public static final String UC_OAUTH_CLIENT_SECRET = "UC_OAUTH_CLIENT_SECRET";

  protected static boolean isUCRemoteConfigured() {
    String ucRemote = System.getenv(UC_REMOTE);
    return ucRemote != null && ucRemote.equalsIgnoreCase("true");
  }

  /** Subclasses can override to false for A/B comparison with the legacy path. */
  protected boolean useDeltaRestApiForTests() {
    return true;
  }

  /**
   * Authentication mode for the LOCAL in-process server. Defaults to OAuth; subclasses override to
   * {@link AuthMode#STATIC} to keep using a static token. OAUTH runs the full server-validating
   * path: authorization is enabled and a {@link MockOAuthBroker} mints an id_token that UC
   * exchanges for an internal token, which UC validates. STATIC uses a static bearer with auth
   * disabled. This does not affect remote runs: a remote server's auth mode is selected by which
   * env vars are set ({@code UC_OAUTH_*} vs {@code UC_TOKEN}), independent of this method.
   */
  protected AuthMode authMode() {
    return AuthMode.OAUTH;
  }

  /**
   * Maximum lifetime (seconds) the mock OAuth broker advertises in its {@code /token} response; any
   * expires_in UC returns is capped at this value. The connector renews ~30s before expiry, so a
   * value at or below that lead time makes it re-run the client-credentials grant (and thus
   * re-exchange) on essentially every call; override low in a test to exercise token renewal.
   */
  protected long oauthTokenMaxLifetimeSeconds() {
    return 3600;
  }

  /**
   * Number of access tokens the local mock OAuth broker has issued. Only a local server in OAUTH
   * mode runs the broker; remote runs authenticate against a real IdP, so there is none to count.
   */
  protected int oauthIssuedTokenCount() {
    Preconditions.checkState(
        mockOAuthBroker != null,
        "oauthIssuedTokenCount() requires the local mock OAuth broker, which runs only for a "
            + "local server in OAUTH mode");
    return mockOAuthBroker.issuedTokenCount();
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

  /** Mock OAuth broker (client-credentials front + OIDC IdP + exchange); started for OAUTH. */
  private MockOAuthBroker mockOAuthBroker;

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
    String schemaName = System.getenv(UC_SCHEMA_NAME);
    String baseTableLocation = System.getenv(UC_BASE_TABLE_LOCATION);
    Preconditions.checkNotNull(serverUri, "%s must be set when UC_REMOTE=true", UC_URI);
    Preconditions.checkNotNull(catalogName, "%s must be set when UC_REMOTE=true", UC_CATALOG_NAME);
    Preconditions.checkNotNull(schemaName, "%s must be set when UC_REMOTE=true", UC_SCHEMA_NAME);
    Preconditions.checkNotNull(
        baseTableLocation, "%s must be set when UC_REMOTE=true", UC_BASE_TABLE_LOCATION);

    // The remote auth mode is chosen by which env vars are set, independent of authMode() (which
    // governs only the local in-process server): set UC_OAUTH_* for client-credentials OAuth, or
    // UC_TOKEN for a static bearer.
    if (System.getenv(UC_OAUTH_URI) != null) {
      // Real OAuth: the connector runs client-credentials against the configured token endpoint
      // (e.g. an external OIDC provider) and the remote server validates the resulting bearer.
      String oauthUri = System.getenv(UC_OAUTH_URI);
      String oauthClientId = System.getenv(UC_OAUTH_CLIENT_ID);
      String oauthClientSecret = System.getenv(UC_OAUTH_CLIENT_SECRET);
      Preconditions.checkNotNull(
          oauthClientId, "%s must be set when UC_OAUTH_URI is set", UC_OAUTH_CLIENT_ID);
      Preconditions.checkNotNull(
          oauthClientSecret, "%s must be set when UC_OAUTH_URI is set", UC_OAUTH_CLIENT_SECRET);
      return new UnityCatalogInfo(
          serverUri,
          catalogName,
          /* serverToken= */ null,
          schemaName,
          baseTableLocation,
          oauthUri,
          oauthClientId,
          oauthClientSecret);
    } else {
      String serverToken = System.getenv(UC_TOKEN);
      Preconditions.checkNotNull(serverToken, "%s must be set when UC_REMOTE=true", UC_TOKEN);
      // Static-token path: no OAuth fields.
      return new UnityCatalogInfo(
          serverUri, catalogName, serverToken, schemaName, baseTableLocation, null, null, null);
    }
  }

  private UnityCatalogInfo localUnityCatalogInfo() {
    Preconditions.checkNotNull(ucServer, "Local Unity Catalog Server is not configured");
    Preconditions.checkNotNull(
        ucBaseTableLocation, "Local Unity Catalog Temp Directory is not configured");
    // Use fake S3 bucket (backed by local filesystem via S3CredentialFileSystem).
    // In OAUTH mode the broker was started in setUpLocalServer, so it must be present; its values
    // drive both the Spark connector's auth config and the scaffolding client. STATIC leaves the
    // OAuth fields null and uses the static token.
    String oauthTokenUri = null;
    String oauthClientId = null;
    String oauthClientSecret = null;
    if (authMode() == AuthMode.OAUTH) {
      Preconditions.checkState(
          mockOAuthBroker != null, "mock OAuth broker must be started for a local OAUTH server");
      oauthTokenUri = mockOAuthBroker.tokenEndpoint();
      oauthClientId = MOCK_OAUTH_CLIENT_ID;
      oauthClientSecret = MOCK_OAUTH_CLIENT_SECRET;
    }
    return new UnityCatalogInfo(
        String.format("http://localhost:%s/", ucServerPort),
        "unity",
        UC_STATIC_TOKEN,
        "default",
        "s3://" + FAKE_S3_BUCKET + ucBaseTableLocation.getAbsolutePath(),
        oauthTokenUri,
        oauthClientId,
        oauthClientSecret);
  }

  /** Finds an available port for the UC server. */
  private int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /**
   * Whether {@code t} (or anything in its cause chain) is an "address already in use" bind failure.
   * The server's async bind surfaces it wrapped as a {@link
   * java.util.concurrent.CompletionException} around a Netty {@code NativeIoException}, so we match
   * by message across the chain.
   */
  private static boolean isAddressInUse(Throwable t) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      final String message = cause.getMessage();
      if (message != null && message.toLowerCase().contains("address already in use")) {
        return true;
      }
    }
    return false;
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

  /**
   * Returns the {@link Properties} used to configure the embedded UC server. Subclasses may
   * override this method, call {@code super.serverProperties()}, and add extra entries.
   */
  protected Properties serverProperties() {
    Properties serverProps = new Properties();
    serverProps.setProperty("server.env", "test");
    serverProps.setProperty("server.managed-table.enabled", "true");
    serverProps.setProperty(
        "storage-root.tables", new File(ucServerDir, "ucroot").getAbsolutePath());
    if (useDeltaRestApiForTests()) {
      serverProps.setProperty("server.managed-table.use-delta-api-only", "true");
    }
    serverProps.setProperty("s3.bucketPath.0", "s3://" + FAKE_S3_BUCKET);
    serverProps.setProperty("s3.accessKey.0", "fakeAccessKey");
    serverProps.setProperty("s3.secretKey.0", "fakeSecretKey");
    serverProps.setProperty("s3.sessionToken.0", "fakeSessionToken");
    if (authMode() == AuthMode.OAUTH) {
      // Server-validating OAuth: enable authorization, trust the broker's issuer, and require the
      // audience it mints.
      serverProps.setProperty("server.authorization", "enable");
      serverProps.setProperty("server.allowed-issuers", mockOAuthBroker.issuer());
      serverProps.setProperty("server.audiences", MockOAuthBroker.AUDIENCE);
    }
    return serverProps;
  }

  private void setUpLocalServer() throws Exception {
    // Create temporary directory for UC server data
    ucServerDir = Files.createTempDirectory("unity-catalog-test-").toFile();
    ucServerDir.deleteOnExit();

    // Create temporary directory for external tables testing.
    ucBaseTableLocation = Files.createTempDirectory("base-table-location-").toFile();
    ucBaseTableLocation.deleteOnExit();

    // Start the mock OAuth broker before building UnityCatalogInfo (and before serverProperties(),
    // which reads the broker's issuer for the allowed-issuers config).
    if (authMode() == AuthMode.OAUTH) {
      mockOAuthBroker =
          new MockOAuthBroker(
              MOCK_OAUTH_CLIENT_ID, MOCK_OAUTH_CLIENT_SECRET, oauthTokenMaxLifetimeSeconds());
      mockOAuthBroker.start();
      LOG.info("Mock OAuth broker started at " + mockOAuthBroker.issuer());
    }

    // Start UC server with configuration. findAvailablePort() picks a free port and releases it,
    // so a concurrently-starting suite on the same runner can grab it before this server binds.
    // Retry with a fresh port on "address already in use" rather than failing the suite.
    ServerProperties initServerProperties = new ServerProperties(serverProperties());
    final int maxBindAttempts = 5;
    for (int attempt = 1; ; attempt++) {
      ucServerPort = findAvailablePort();
      UnityCatalogServer server =
          UnityCatalogServer.builder()
              .port(ucServerPort)
              .serverProperties(initServerProperties)
              .build();
      try {
        server.start();
        ucServer = server;
        break;
      } catch (Exception e) {
        if (attempt < maxBindAttempts && isAddressInUse(e)) {
          LOG.warn(
              String.format(
                  "UC server bind on port %d failed (address in use), retrying (attempt %d/%d)",
                  ucServerPort, attempt, maxBindAttempts));
          continue;
        }
        throw e;
      }
    }

    // Now that UC is up, let the OAuth broker reach the token-exchange endpoint.
    if (mockOAuthBroker != null) {
      mockOAuthBroker.setUcBaseUri(String.format("http://localhost:%s/", ucServerPort));
    }

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

    if (mockOAuthBroker != null) {
      mockOAuthBroker.stop();
      LOG.info("Mock OAuth broker stopped");
      mockOAuthBroker = null;
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

  /** Returns Unity Catalog Spark version, like [0, 4, 0]. */
  protected static int[] getUnityCatalogSparkVersion() {
    String version = Preconditions.checkNotNull(VersionUtils.VERSION);
    String[] parts = version.split("[.\\-]", 4);
    int major = Integer.parseInt(parts[0]);
    int minor = Integer.parseInt(parts[1]);
    int patch = Integer.parseInt(parts[2]);
    return new int[] {major, minor, patch};
  }

  /** Returns whether the Unity Catalog Spark version is at least {@code major.minor.patch}. */
  protected static boolean isUnityCatalogSparkAtLeast(int major, int minor, int patch) {
    return Arrays.compare(getUnityCatalogSparkVersion(), new int[] {major, minor, patch}) >= 0;
  }
}
