package io.delta.flink.internal.table;

import java.util.Map;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.CatalogFactory.Context;

/**
 * Basic implementation of Flink's {@link CatalogFactory.Context} that is needed as an argument of
 * Flink's {@link CatalogFactory#createCatalog(Context)} used by {@link CatalogLoader}.
 * <p>
 * All Flink's implementations of {@link CatalogFactory.Context} are marked as {@code @Internal} so
 * not meant to be used by users. This implementation is based on Flink's {@link
 * org.apache.flink.table.factories.FactoryUtil.DefaultCatalogContext}
 */
public class DeltaCatalogContext implements CatalogFactory.Context {

    private final String catalogName;

    private final Map<String, String> options;

    private final ReadableConfig configuration;

    private final ClassLoader classLoader;

    public DeltaCatalogContext(
            String catalogName,
            Map<String, String> options,
            ReadableConfig configuration,
            ClassLoader classLoader) {
        this.catalogName = catalogName;
        this.options = options;
        this.configuration = configuration;
        this.classLoader = classLoader;
    }

    @Override
    public String getName() {
        return catalogName;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public ReadableConfig getConfiguration() {
        return configuration;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
