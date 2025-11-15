package io.delta.flink.internal.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.FactoryUtil;

public final class DeltaTableFactoryHelper {

    private DeltaTableFactoryHelper() {}

    /**
     * Options to exclude from job-specific option validation during processing SELECT/INSERT
     * queries. Thees options are needed to properly select and prepare sink/source builders. In
     * case of {@link FactoryUtil#CONNECTOR} redirect further execution to proper table factory
     * based on connector type. At the same time thees options may not be supported by
     * builder.option(...). If passed directly to the builder with rest of the job-specific options,
     * builder could throw an exception.
     */
    private static final Set<String> OPTIONS_TO_IGNORE = Stream.of(
        FactoryUtil.CONNECTOR.key(),
        DeltaTableConnectorOptions.TABLE_PATH.key(),
        DeltaFlinkJobSpecificOptions.MODE.key()
    ).collect(Collectors.toSet());

    /**
     * Validates options defined using query hints defined for SELECT statement. This method will
     * compare options defined by query hints with
     * {@link DeltaFlinkJobSpecificOptions#SOURCE_JOB_OPTIONS}.
     * This method will ignore options like 'tablePath' that will be provided by Delta Catalog.
     * Options to ignore are defined in {@link DeltaTableFactoryHelper#OPTIONS_TO_IGNORE}
     *
     * @param options options to validate.
     * @return A {@link QueryOptions} containing options for given SELECT statement.
     * @throws ValidationException in case of invalid job-specific options were used.
     */
    public static QueryOptions validateSourceQueryOptions(Configuration options) {

        validateDeltaTablePathOption(options);

        Map<String, String> jobSpecificOptions = new HashMap<>();

        List<String> invalidOptions = new ArrayList<>();
        for (Entry<String, String> entry : options.toMap().entrySet()) {
            String optionName = entry.getKey();

            if (OPTIONS_TO_IGNORE.contains(optionName)) {
                // skip mandatory options
                continue;
            }

            if (DeltaFlinkJobSpecificOptions.SOURCE_JOB_OPTIONS.contains(optionName)) {
                jobSpecificOptions.put(optionName, entry.getValue());
            } else {
                invalidOptions.add(optionName);
            }
        }

        if (!invalidOptions.isEmpty()) {
            throw CatalogExceptionHelper.invalidSelectJobPropertyException(invalidOptions);
        }

        return new QueryOptions(
            options.get(DeltaTableConnectorOptions.TABLE_PATH),
            options.get(DeltaFlinkJobSpecificOptions.MODE),
            jobSpecificOptions
        );
    }

    /**
     * Validates options defined using query hints defined for INSERT statement. Currently, no job
     * specific options are allowed for INSERT statements.
     * <p>
     * This method will ignore options like 'tablePath' that will be provided by Delta Catalog.
     * Options to ignore are defined in {@link DeltaTableFactoryHelper#OPTIONS_TO_IGNORE}
     *
     * @param options options to validate.
     * @return A {@link QueryOptions} containing options for given INSERT statement.
     * @throws ValidationException in case of invalid job-specific options were used.
     */
    public static QueryOptions validateSinkQueryOptions(Configuration options) {

        validateDeltaTablePathOption(options);

        Map<String, String> jobSpecificOptions = new HashMap<>();

        List<String> invalidOptions = new ArrayList<>();
        for (Entry<String, String> entry : options.toMap().entrySet()) {
            String optionName = entry.getKey();

            if (OPTIONS_TO_IGNORE.contains(optionName)) {
                // skip mandatory options
                continue;
            }

            // currently, no job-specific options are supported for sink.
            invalidOptions.add(optionName);
        }

        if (!invalidOptions.isEmpty()) {
            throw CatalogExceptionHelper.invalidInsertJobPropertyException(invalidOptions);
        }

        return new QueryOptions(
            options.get(DeltaTableConnectorOptions.TABLE_PATH),
            options.get(DeltaFlinkJobSpecificOptions.MODE),
            jobSpecificOptions
        );
    }

    public static void validateDeltaTablePathOption(Configuration options) {
        if (!options.contains(DeltaTableConnectorOptions.TABLE_PATH)) {
            throw new ValidationException("Missing path to Delta table");
        }
    }

}
