package io.delta.flink.internal.table;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalogFactory;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory;
import org.apache.flink.table.factories.CatalogFactory.Context;
import static io.delta.flink.internal.table.DeltaCatalogFactory.CATALOG_TYPE;

/**
 * Creates a concrete catalog instance that will be used as decorated catalog by {@link
 * DeltaCatalog}.
 */
public interface CatalogLoader extends Serializable {

    Catalog createCatalog(Context context);

    /**
     * @return Catalog loader for Flink's
     * {@link org.apache.flink.table.catalog.GenericInMemoryCatalog}.
     */
    static CatalogLoader inMemory() {
        return new InMemoryCatalogLoader();
    }

    /**
     * @return Catalog loader for Flink's {@link org.apache.flink.table.catalog.hive.HiveCatalog}.
     */
    static CatalogLoader hive() {
        return new HiveCatalogLoader();
    }

    /**
     * A catalog loader that creates Flink's
     * {@link org.apache.flink.table.catalog.GenericInMemoryCatalog}
     * instance that will be used by {@link DeltaCatalog} as a metastore and to proxy none Delta
     * related queries to.
     */
    class InMemoryCatalogLoader implements CatalogLoader {

        @Override
        public Catalog createCatalog(Context context) {
            Context newContext = filterDeltaCatalogOptions(context);
            return new GenericInMemoryCatalogFactory().createCatalog(newContext);
        }
    }

    /**
     * A catalog loader that creates Flink's {@link org.apache.flink.table.catalog.hive.HiveCatalog}
     * instance that will be used by {@link DeltaCatalog} as a metastore and to proxy none Delta
     * related queries to.
     */
    class HiveCatalogLoader implements CatalogLoader {

        @Override
        public Catalog createCatalog(Context context) {
            Context newContext = filterDeltaCatalogOptions(context);
            // Connectors like Iceberg have its own Hive Catalog implementation and his own
            // Catalog "like" interface currently we are reusing Flink's classes.

            // We had to add extra dependency to have access to HiveCatalogFactory.
            // "org.apache.flink" % "flink-connector-hive_2.12" % flinkVersion % "provided",
            // "org.apache.flink" % "flink-table-planner_2.12" % flinkVersion % "provided",
            // and remove "org.apache.flink" % "flink-table-test-utils" % flinkVersion % "test",
            // but this causes delta CI to fail for scala 2.11.12 that is way, after this change
            // Flink connector will not be build on scala 2.11.12.
            return new HiveCatalogFactory().createCatalog(newContext);
        }
    }

    /**
     * This method removes all Delta Catalog related options such as 'catalog-type' from {@link
     * Context}. If those options would not be removed, then underlying Catalog Factory might from
     * exception due to unexpected configuration option.
     *
     * @param context context form which Delta Catalog options should be filter out.
     * @return context having no Delta Catalog related options.
     */
    default Context filterDeltaCatalogOptions(Context context) {
        Map<String, String> filteredOptions = new HashMap<>(context.getOptions());
        filteredOptions.remove(CATALOG_TYPE);

        return new DeltaCatalogContext(
            context.getName(),
            filteredOptions,
            context.getConfiguration(),
            context.getClassLoader()
        );
    }
}
