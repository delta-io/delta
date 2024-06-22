package io.delta.flink.utils.extensions.hive;

import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.jupiter.api.extension.ExtensionContext;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CONNECT_URL_KEY;

/**
 * A JUnit Extension that creates a Hive Metastore backed by an in-memory database.
 * <p>
 * A fresh database instance will be created for each test method.
 * </p>
 *
 * @implNote This class is based on https://github.com/ExpediaGroup/beeju/blob/beeju-5.0
 * .0/src/main/java/com/hotels/beeju/extensions/HiveMetaStoreJUnitExtension.java
 * and "trimmed" to our needs. We could not use entire beeju library as sbt dependency due to
 * Dependency conflicts with Flink on Calcite, Parquet and many others. See https://github
 * .com/ExpediaGroup/beeju/issues/54 for details. As a result we added only org .apache.hive
 * hive-exec and hive-metastore dependencies, and we used beeju's Junit5 extension classes.
 */
public class HiveMetaStoreJUnitExtension extends HiveJUnitExtension {

    private final HiveMetaStoreCore hiveMetaStoreCore;

    /**
     * Create a Hive Metastore with a pre-created database using the provided name and
     * configuration.
     *
     * @param databaseName  Database name.
     * @param configuration Hive configuration properties.
     */
    public HiveMetaStoreJUnitExtension(String databaseName, Map<String, String> configuration) {
        super(databaseName, configuration);
        hiveMetaStoreCore = new HiveMetaStoreCore(core);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        System.clearProperty(CONNECT_URL_KEY.getVarname());
        super.beforeEach(context);
        hiveMetaStoreCore.initialise();
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        hiveMetaStoreCore.shutdown();
        super.afterEach(context);
    }

    /**
     * @return {@link HiveMetaStoreCore#client()}.
     */
    public HiveMetaStoreClient client() {
        return hiveMetaStoreCore.client();
    }
}
