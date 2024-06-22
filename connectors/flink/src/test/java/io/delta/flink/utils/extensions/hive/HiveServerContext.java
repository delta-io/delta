package io.delta.flink.utils.extensions.hive;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class contains some code sourced from and inspired by HiveRunner, specifically
 * https://github.com/klarna/HiveRunner/blob/fb00a98f37abdb779547c1c98ef6fbe54d373e0c/src/main
 * /java/com/klarna/hiverunner/StandaloneHiveServerContext.java
 *
 * @implNote This class is based on https://github.com/ExpediaGroup/beeju/blob/beeju-5.0
 * .0/src/main/java/com/hotels/beeju/core/BeejuCore.java and "trimmed" to our needs. We could not
 * use entire beeju library as sbt dependency due to Dependency conflicts with Flink on Calcite,
 * Parquet and many others. See https://github.com/ExpediaGroup/beeju/issues/54 for details. As a
 * result we added only org .apache.hive hive-exec and hive-metastore dependencies, and we used
 * beeju's Junit5 extension classes.
 */
public class HiveServerContext {

    private static final Logger log = LoggerFactory.getLogger(HiveServerContext.class);

    // "user" conflicts with USER db and the metastore_db can't be created.
    private static final String METASTORE_DB_USER = "db_user";

    private static final String METASTORE_DB_PASSWORD = "db_password";

    protected final HiveConf conf = new HiveConf();

    private final String databaseName;

    private Path warehouseDir;

    private Path baseDir;

    public HiveServerContext(String databaseName, Map<String, String> preConfiguration) {
        this(databaseName, preConfiguration, Collections.emptyMap());
    }

    public HiveServerContext(
            String databaseName, Map<String,
            String> preConfiguration,
            Map<String, String> postConfiguration) {
        checkNotNull(databaseName, "databaseName is required");
        this.databaseName = databaseName;
        configure(preConfiguration);

        configureFolders();

        configureMetastore();

        configureMisc();

        configure(postConfiguration);
    }

    private void configureMisc() {
        int webUIPort = getWebUIPort();

        // override default port as some of our test environments claim it is in use.
        conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, webUIPort);

        conf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);

        // Disable to get rid of clean up exception when stopping the Session.
        conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);

        // Used to prevent "Not authorized to make the get_current_notificationEventId call" errors
        setMetastoreAndSystemProperty(MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH,
            "false");

        // Used to prevent "Error polling for notification events" error
        conf.setTimeVar(HiveConf.ConfVars.HIVE_NOTFICATION_EVENT_POLL_INTERVAL, 0,
            TimeUnit.MILLISECONDS);

        // Has to be added to exclude failures related to the HiveMaterializedViewsRegistry
        conf.set(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname, "DUMMY");
        System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname,
            "DUMMY");
    }

    private void setMetastoreAndSystemProperty(MetastoreConf.ConfVars key, String value) {
        conf.set(key.getVarname(), value);
        conf.set(key.getHiveName(), value);

        System.setProperty(key.getVarname(), value);
        System.setProperty(key.getHiveName(), value);
    }

    private int getWebUIPort() {
        // Try to find a free port, if impossible return the default port 0 which disables the
        // WebUI altogether
        int defaultPort = 0;

        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            log.info(
                "No free port available for the Web UI. Setting the port to " + defaultPort
                    + ", which disables the WebUI.",
                e);
            return defaultPort;
        }
    }

    private void configureFolders() {
        try {
            baseDir = Files.createTempDirectory("hive-basedir-");
            createAndSetFolderProperty(HiveConf.ConfVars.SCRATCHDIR, "scratchdir");
            createAndSetFolderProperty(HiveConf.ConfVars.LOCALSCRATCHDIR, "localscratchdir");
            createAndSetFolderProperty(HiveConf.ConfVars.HIVEHISTORYFILELOC, "hive-history");

            createDerbyPaths();
            createWarehousePath();
        } catch (IOException e) {
            throw new UncheckedIOException("Error creating temporary folders", e);
        }
    }

    private void configureMetastore() {
        String driverClassName = EmbeddedDriver.class.getName();
        conf.setBoolean("hcatalog.hive.client.cache.disabled", true);
        String connectionURL = "jdbc:derby:memory:" + UUID.randomUUID() + ";create=true";

        setMetastoreAndSystemProperty(MetastoreConf.ConfVars.CONNECT_URL_KEY, connectionURL);
        setMetastoreAndSystemProperty(MetastoreConf.ConfVars.CONNECTION_DRIVER, driverClassName);
        setMetastoreAndSystemProperty(MetastoreConf.ConfVars.CONNECTION_USER_NAME,
            METASTORE_DB_USER);
        setMetastoreAndSystemProperty(MetastoreConf.ConfVars.PWD, METASTORE_DB_PASSWORD);

        conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, "NONE");
        conf.setBoolVar(HiveConf.ConfVars.HMSHANDLERFORCERELOADCONF, true);

        // Hive 2.x compatibility
        setMetastoreAndSystemProperty(MetastoreConf.ConfVars.AUTO_CREATE_ALL, "true");
        setMetastoreAndSystemProperty(MetastoreConf.ConfVars.SCHEMA_VERIFICATION, "false");
    }

    private void createAndSetFolderProperty(HiveConf.ConfVars var, String childFolderName)
        throws IOException {
        String folderPath = newFolder(baseDir, childFolderName).toAbsolutePath().toString();
        conf.setVar(var, folderPath);
    }

    private Path newFolder(Path basedir, String folder) throws IOException {
        Path newFolder = Files.createTempDirectory(basedir, folder);
        FileUtil.setPermission(newFolder.toFile(), FsPermission.getDirDefault());
        return newFolder;
    }

    private void createDerbyPaths() throws IOException {
        Path derbyHome = Files.createTempDirectory(baseDir, "derby-home-");
        System.setProperty("derby.system.home", derbyHome.toString());

        // overriding default derby log path to go to tmp
        String derbyLog = Files.createTempFile(baseDir, "derby", ".log").toString();
        System.setProperty("derby.stream.error.file", derbyLog);
    }

    private void createWarehousePath() throws IOException {
        warehouseDir = Files.createTempDirectory(baseDir, "hive-warehouse-");
        setHiveVar(HiveConf.ConfVars.METASTOREWAREHOUSE, warehouseDir.toString());
    }

    public void cleanUp() {
        deleteDirectory(baseDir);
    }

    private void deleteDirectory(Path path) {
        try {
            FileUtils.deleteDirectory(path.toFile());
        } catch (IOException e) {
            log.warn("Error cleaning up " + path, e);
        }
    }

    private void configure(Map<String, String> customConfiguration) {
        if (customConfiguration != null) {
            for (Map.Entry<String, String> entry : customConfiguration.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }
        }
    }

    void setHiveVar(HiveConf.ConfVars variable, String value) {
        conf.setVar(variable, value);
    }

    /**
     * Create a new database with the specified name.
     *
     * @param databaseName Database name.
     * @throws TException If an error occurs creating the database.
     */
    public void createDatabase(String databaseName) throws TException {
        File tempFile = warehouseDir.toFile();
        String databaseFolder = new File(tempFile, databaseName).toURI().toString();

        try (HiveMetaStoreClient client = newClient()) {
            client.createDatabase(new Database(databaseName, null, databaseFolder, null));
        }
    }

    /**
     * @return a copy of the {@link HiveConf} used to create the Hive Metastore database. This
     * {@link HiveConf} should be used by tests wishing to connect to the database.
     */
    public HiveConf conf() {
        return new HiveConf(conf);
    }

    /**
     * @return the name of the pre-created database.
     */
    public String databaseName() {
        return databaseName;
    }

    /**
     * Creates a new HiveMetaStoreClient that can talk directly to the backed metastore database.
     * <p>
     * The invoker is responsible for closing the client.
     * </p>
     *
     * @return the {@link HiveMetaStoreClient} backed by an HSQLDB in-memory database.
     */
    public HiveMetaStoreClient newClient() {
        try {
            return new HiveMetaStoreClient(conf);
        } catch (MetaException e) {
            throw new RuntimeException("Unable to create HiveMetaStoreClient", e);
        }
    }
}
