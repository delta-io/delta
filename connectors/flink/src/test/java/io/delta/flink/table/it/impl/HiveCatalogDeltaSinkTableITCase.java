package io.delta.flink.table.it.impl;

import io.delta.flink.table.it.suite.DeltaSinkTableTestSuite;
import io.delta.flink.utils.extensions.HiveCatalogExtension;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.extension.RegisterExtension;

public class HiveCatalogDeltaSinkTableITCase extends DeltaSinkTableTestSuite {

    @RegisterExtension
    private final HiveCatalogExtension catalogExtension = new HiveCatalogExtension();

    @Override
    public void setupDeltaCatalog(TableEnvironment tableEnv) {
        catalogExtension.setupDeltaCatalog(tableEnv);
    }
}
