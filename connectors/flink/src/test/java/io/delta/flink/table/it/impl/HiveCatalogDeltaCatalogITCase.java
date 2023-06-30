package io.delta.flink.table.it.impl;

import io.delta.flink.table.it.suite.DeltaCatalogTestSuite;
import io.delta.flink.utils.extensions.HiveCatalogExtension;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.extension.RegisterExtension;

public class HiveCatalogDeltaCatalogITCase extends DeltaCatalogTestSuite {

    @RegisterExtension
    private final HiveCatalogExtension catalogExtension = new HiveCatalogExtension();

    public void setupDeltaCatalog(TableEnvironment tableEnv) {
        catalogExtension.setupDeltaCatalog(tableEnv);
    }
}
