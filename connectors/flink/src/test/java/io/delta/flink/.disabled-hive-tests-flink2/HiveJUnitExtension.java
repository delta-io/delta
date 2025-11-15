package io.delta.flink.utils.extensions.hive;

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.thrift.TException;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Base class for JUnit Extensions that require a Hive Metastore database configuration pre-set.
 *
 * @implNote This class is based on from https://github.com/ExpediaGroup/beeju/blob/beeju-5.0
 * .0/src/main/java/com/hotels/beeju/extensions/BeejuJUnitExtension.java
 * and "trimmed" to our needs. We could not use entire beeju library as sbt dependency due to
 * Dependency conflicts with Flink on Calcite, Parquet and many others. See https://github
 * .com/ExpediaGroup/beeju/issues/54 for details. As a result we added only org .apache.hive
 * hive-exec and hive-metastore dependencies, and we used beeju's Junit5 extension classes.
 */
public abstract class HiveJUnitExtension implements BeforeEachCallback, AfterEachCallback {

    protected HiveServerContext core;

    public HiveJUnitExtension(String databaseName, Map<String, String> configuration) {
        core = new HiveServerContext(databaseName, configuration);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        createDatabase(databaseName());
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        core.cleanUp();
    }

    /**
     * @return {@link HiveServerContext#databaseName()}.
     */
    public String databaseName() {
        return core.databaseName();
    }

    /**
     * @return {@link HiveServerContext#conf()}.
     */
    public HiveConf conf() {
        return core.conf();
    }

    /**
     * See {@link HiveServerContext#createDatabase(String)}
     *
     * @param databaseName Database name.
     * @throws TException If an error occurs creating the database.
     */
    public void createDatabase(String databaseName) throws TException {
        core.createDatabase(databaseName);
    }
}
