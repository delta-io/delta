package io.delta.flink.internal.table;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * The catalog factory implementation for Delta Catalog. This factory will be discovered by Flink
 * runtime using Java’s Service Provider Interfaces (SPI) based on
 * resources/META-INF/services/org.apache.flink.table.factories.Factory file.
 * <p>
 * Flink runtime will call {@link #createCatalog(Context)} method that will return new Delta Catalog
 * instance.
 */
public class DeltaCatalogFactory implements CatalogFactory {

    /**
     * Option to choose what should be the decorated catalog type.
     */
    public static final String CATALOG_TYPE = "catalog-type";

    /**
     * Value for "catalog-type" catalog option that will make Delta Catalog use Flink's Hive Catalog
     * as its decorated catalog.
     */
    public static final String CATALOG_TYPE_HIVE = "hive";

    /**
     * Value for "catalog-type" catalog option that will make Delta Catalog use Flink's In-memory
     * catalog as its decorated catalog.
     */
    public static final String CATALOG_TYPE_IN_MEMORY = "in-memory";

    /**
     * Property with default database name used for metastore entries.
     */
    public static final ConfigOption<String> DEFAULT_DATABASE =
        ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
            .stringType()
            .defaultValue("default");

    /**
     * Creates and configures a Catalog using the given context
     *
     * @param context {@link Context} object containing catalog properties.
     * @return new instance of Delta Catalog.
     */
    @Override
    public Catalog createCatalog(Context context) {
        Map<String, String> originalOptions = context.getOptions();

        // Since we want to add extra options here, to avoid any unintentional mutation of the
        // input context object and causing added options to leak out we are creating a working
        // copy of original option map. We are playing safe here, making sure nothing, that has
        // Delta Catalog scope will leak out.
        Map<String, String> deltaContextOptions = new HashMap<>(originalOptions);

        // Making sure that decorated catalog will use the same name for default database.
        if (!deltaContextOptions.containsKey(CommonCatalogOptions.DEFAULT_DATABASE_KEY)) {
            deltaContextOptions.put(DEFAULT_DATABASE.key(), DEFAULT_DATABASE.defaultValue());
        }

        DeltaCatalogContext deltaCatalogContext = new DeltaCatalogContext(
            context.getName(),
            deltaContextOptions,
            context.getConfiguration(),
            context.getClassLoader()
        );

        Catalog decoratedCatalog = createDecoratedCatalog(deltaCatalogContext);
        Configuration hadoopConfiguration =
            HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
        return new CatalogProxy(context.getName(), "default", decoratedCatalog,
            hadoopConfiguration);
    }

    @Override
    public String factoryIdentifier() {
        return "delta-catalog";
    }

    private Catalog createDecoratedCatalog(Context context) {

        Map<String, String> options = context.getOptions();
        String catalogType = options.getOrDefault(CATALOG_TYPE, CATALOG_TYPE_IN_MEMORY);

        switch (catalogType.toLowerCase(Locale.ENGLISH)) {
            case CATALOG_TYPE_HIVE:
                return CatalogLoader.hive().createCatalog(context);
            case CATALOG_TYPE_IN_MEMORY:
                return CatalogLoader.inMemory().createCatalog(context);
            default:
                throw new CatalogException("Unknown catalog-type: " + catalogType +
                    " (Must be 'hive' or 'inMemory')");
        }

    }
}
