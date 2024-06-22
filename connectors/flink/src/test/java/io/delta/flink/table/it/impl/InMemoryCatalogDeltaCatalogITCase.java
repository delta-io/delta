package io.delta.flink.table.it.impl;

import io.delta.flink.table.it.suite.DeltaCatalogTestSuite;
import io.delta.flink.utils.extensions.InMemoryCatalogExtension;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Configuration class for {@link DeltaCatalogTestSuite}.
 * Runs all tests from DeltaCatalogTestSuite with {@link io.delta.flink.internal.table.DeltaCatalog}
 * using 'In Memory' metastore.
 */
public class InMemoryCatalogDeltaCatalogITCase extends DeltaCatalogTestSuite {

    @RegisterExtension
    private final InMemoryCatalogExtension catalogExtension = new InMemoryCatalogExtension();

    public void setupDeltaCatalog(TableEnvironment tableEnv) {
        catalogExtension.setupDeltaCatalog(tableEnv);
    }
}
