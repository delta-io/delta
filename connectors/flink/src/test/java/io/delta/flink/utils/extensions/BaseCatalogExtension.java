package io.delta.flink.utils.extensions;

import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;

/**
 * Implementations of this class should prepare test environment by setting up Delta Catalog. Delta
 * Catalog created by this extension will be used for all tests where this extension is used.
 */
public abstract class BaseCatalogExtension implements BeforeEachCallback, AfterEachCallback {

    /**
     * Setup Delta Catalog for test run.
     * @param tableEnv {@link TableEnvironment} used for test run.
     */
    public abstract void setupDeltaCatalog(TableEnvironment tableEnv);

}
