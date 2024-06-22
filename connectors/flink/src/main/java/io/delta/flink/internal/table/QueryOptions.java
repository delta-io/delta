package io.delta.flink.internal.table;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.delta.flink.internal.table.DeltaFlinkJobSpecificOptions.QueryMode;

/**
 * Data object containing information about Delta table path, query mode (streaming or batch) and
 * used job-specific options such as startingVersion, versionAsOf etc.
 */
public class QueryOptions {

    /**
     * Path to Delta table.
     */
    private final String deltaTablePath;

    /**
     * Selected query mode for query.
     */
    private final QueryMode queryMode;

    /**
     * Job-specific options for given query defined as query hint.
     * Map's key represents an option name, map's value represent an option value.
     */
    private final Map<String, String> jobSpecificOptions = new HashMap<>();

    public QueryOptions(
        String deltaTablePath,
        QueryMode queryMode,
        Map<String, String> jobSpecificOptions) {
        this.deltaTablePath = deltaTablePath;
        this.queryMode = queryMode;
        this.jobSpecificOptions.putAll(jobSpecificOptions);
    }

    public String getDeltaTablePath() {
        return deltaTablePath;
    }

    public QueryMode getQueryMode() {
        return queryMode;
    }

    /**
     * @return an unmodifiable {@code java.util.Map} containing job-specific options.
     */
    public Map<String, String> getJobSpecificOptions() {
        return Collections.unmodifiableMap(jobSpecificOptions);
    }

}
