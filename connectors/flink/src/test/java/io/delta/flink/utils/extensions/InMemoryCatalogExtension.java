package io.delta.flink.utils.extensions;

import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * A Junit test extension that setup Delta Catalog with 'In Memory' metastore.
 */
public class InMemoryCatalogExtension extends BaseCatalogExtension {

    @Override
    public void setupDeltaCatalog(TableEnvironment tableEnv) {
        String catalogSQL = "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog');";
        String useDeltaCatalog = "USE CATALOG myDeltaCatalog;";

        tableEnv.executeSql(catalogSQL);
        tableEnv.executeSql(useDeltaCatalog);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        // Nothing to do here for this extension.
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        // Nothing to do here for this extension.
    }
}
