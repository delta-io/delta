package io.delta.flink.table.it.impl;

import io.delta.flink.table.it.suite.DeltaFlinkSqlTestSuite;
import io.delta.flink.utils.extensions.HiveCatalogExtension;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.extension.RegisterExtension;

public class HiveCatalogDeltaFlinkSqlITCase extends DeltaFlinkSqlTestSuite {

    @RegisterExtension
    private final HiveCatalogExtension catalogExtension = new HiveCatalogExtension();

    @Override
    public void setupDeltaCatalog(TableEnvironment tableEnv) {
        catalogExtension.setupDeltaCatalog(tableEnv);
    }
}
