package io.delta.flink.utils.extensions.hive;

import java.util.Map;

import org.junit.jupiter.api.extension.ExtensionContext;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CONNECT_URL_KEY;

/**
 * A JUnit Extension that creates a Hive Metastore Thrift service backed by a Hive Metastore using
 * an in-memory database.
 * <p>
 * A fresh database instance will be created for each test method.
 * </p>
 *
 * @implNote This class is based on https://github.com/ExpediaGroup/beeju/blob/beeju-5.0
 * .0/src/main/java/com/hotels/beeju/extensions/ThriftHiveMetaStoreJUnitExtension.java and
 * "trimmed" to our needs. We could not use entire beeju library as sbt dependency due to Dependency
 * conflicts with Flink on Calcite, Parquet and many others. See https://github
 * .com/ExpediaGroup/beeju/issues/54 for details. As a result we added only org .apache.hive
 * hive-exec and hive-metastore dependencies, and we used beeju's Junit5 extension classes.
 */
public class ThriftHiveMetaStoreJUnitExtension extends HiveMetaStoreJUnitExtension {

    private final ThriftHiveMetaStoreCore thriftHiveMetaStoreCore;

    /**
     * Create a Thrift Hive Metastore service with a pre-created database using the provided name.
     *
     * @param databaseName Database name.
     */
    public ThriftHiveMetaStoreJUnitExtension(String databaseName) {
        this(databaseName, null);
    }

    /**
     * Create a Thrift Hive Metastore service with a pre-created database using the provided name
     * and configuration.
     *
     * @param databaseName  Database name.
     * @param configuration Hive configuration properties.
     */
    public ThriftHiveMetaStoreJUnitExtension(
            String databaseName,
            Map<String, String> configuration) {
        super(databaseName, configuration);
        thriftHiveMetaStoreCore = new ThriftHiveMetaStoreCore(core);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        System.clearProperty(CONNECT_URL_KEY.getVarname());
        thriftHiveMetaStoreCore.initialise();
        super.beforeEach(context);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        thriftHiveMetaStoreCore.shutdown();
        super.afterEach(context);
    }

    /**
     * @return {@link ThriftHiveMetaStoreCore#getThriftConnectionUri()}.
     */
    public String getThriftConnectionUri() {
        return thriftHiveMetaStoreCore.getThriftConnectionUri();
    }

    /**
     * @return {@link ThriftHiveMetaStoreCore#getThriftPort()}
     */
    public int getThriftPort() {
        return thriftHiveMetaStoreCore.getThriftPort();
    }

    /**
     * @param thriftPort The Port to use for the Thrift Hive metastore, if not set then a port
     *                   number will automatically be allocated.
     */
    public void setThriftPort(int thriftPort) {
        thriftHiveMetaStoreCore.setThriftPort(thriftPort);
    }
}
