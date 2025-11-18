package io.delta.flink.internal.table;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalogFactory;
import org.apache.flink.table.factories.CatalogFactory.Context;
import static io.delta.flink.internal.table.DeltaCatalogFactory.CATALOG_TYPE;

/*
 * FLINK 2.0 MIGRATION STATUS: Hive Catalog Support
 *
 * STATUS: ⏳ PENDING FUTURE IMPLEMENTATION
 *
 * BACKGROUND:
 * The Hive Catalog integration requires 'flink-connector-hive' dependency, which may have
 * changed or been reorganized in Flink 2.0. The previous import location was:
 *   org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory
 *
 * CURRENT STATE:
 * - InMemoryCatalog (GenericInMemoryCatalog): ✅ FULLY WORKING
 * - HiveCatalog: ⏳ TEMPORARILY DISABLED
 *
 * IMPACT:
 * This does NOT block the Delta Flink connector functionality. Users can:
 * 1. Use InMemoryCatalog for all operations (default, recommended)
 * 2. Wait for Hive Catalog support to be re-enabled in a future update
 *
 * NEXT STEPS TO RE-ENABLE:
 * 1. Add 'flink-connector-hive' dependency to build.sbt (if available for Flink 2.0)
 * 2. Investigate new HiveCatalogFactory location/API in Flink 2.0
 * 3. Update HiveCatalogLoader implementation
 * 4. Migrate Hive-related tests (they use HiveMetaStoreClient, HiveConf, etc.)
 * 5. Update documentation with Hive Catalog configuration
 *
 * REFERENCES:
 * - Issue: #5228
 * - Related tests: HiveCatalogDeltaSourceTableITCase, HiveCatalogExtension
 */

/**
 * Creates a concrete catalog instance that will be used as decorated catalog by {@link
 * DeltaCatalog}.
 */
public interface CatalogLoader extends Serializable {

    Catalog createCatalog(Context context);

    /**
     * @return Catalog loader for Flink's
     * {@link org.apache.flink.table.catalog.GenericInMemoryCatalog}.
     * This is the recommended catalog type for Flink 2.0 and is fully supported.
     */
    static CatalogLoader inMemory() {
        return new InMemoryCatalogLoader();
    }

    /**
     * @return Catalog loader for Flink's {@link org.apache.flink.table.catalog.hive.HiveCatalog}.
     * <p>
     * <strong>IMPORTANT:</strong> Hive Catalog is temporarily disabled in Flink 2.0 migration.
     * This feature is pending future implementation as it requires 'flink-connector-hive'
     * dependency and API updates for Flink 2.0.
     * </p>
     * <p>
     * <strong>WORKAROUND:</strong> Use {@link #inMemory()} catalog instead. All Delta Lake
     * operations work correctly with InMemoryCatalog.
     * </p>
     *
     * @throws UnsupportedOperationException always, until Hive support is re-enabled
     */
    static CatalogLoader hive() {
        throw new UnsupportedOperationException(
            "Hive Catalog is temporarily disabled in Flink 2.0 migration. " +
            "This feature requires 'flink-connector-hive' dependency and API updates. " +
            "Use inMemory() catalog instead, which is fully supported. " +
            "For more information, see: https://github.com/delta-io/delta/issues/5228");
        // return new HiveCatalogLoader();
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
     * <p>
     * <strong>DISABLED IN FLINK 2.0:</strong> This class is temporarily non-functional pending
     * migration to Flink 2.0 Hive Catalog APIs.
     * </p>
     * <p>
     * <strong>REQUIREMENTS FOR RE-ENABLING:</strong>
     * </p>
     * <ul>
     *   <li>Add dependency: "org.apache.flink" % "flink-connector-hive" % flinkVersion
     *       % "provided"</li>
     *   <li>Add dependency: "org.apache.flink" % "flink-table-planner_2.12" % flinkVersion
     *       % "provided"</li>
     *   <li>Find new HiveCatalogFactory location in Flink 2.0 (package may have changed)</li>
     *   <li>Update createCatalog() implementation for Flink 2.0 APIs</li>
     * </ul>
     * <p>
     * <strong>NOTE:</strong> Connectors like Iceberg have their own Hive Catalog implementation.
     * We currently reuse Flink's classes but may need to create a custom implementation.
     * </p>
     */
    class HiveCatalogLoader implements CatalogLoader {

        @Override
        public Catalog createCatalog(Context context) {
            throw new UnsupportedOperationException(
                "Hive Catalog is temporarily disabled in Flink 2.0 migration. " +
                "This feature requires 'flink-connector-hive' dependency and API updates. " +
                "Use inMemory() catalog instead, which is fully supported. " +
                "For more information, see: https://github.com/delta-io/delta/issues/5228");

            // ORIGINAL IMPLEMENTATION (commented out for Flink 2.0):
            //
            // Context newContext = filterDeltaCatalogOptions(context);
            // return new HiveCatalogFactory().createCatalog(newContext);
            //
            // NOTE: HiveCatalogFactory location was:
            //   org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory (Flink 1.x)
            // New location for Flink 2.0 needs investigation.
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
