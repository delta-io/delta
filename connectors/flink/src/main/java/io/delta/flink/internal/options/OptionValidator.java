package io.delta.flink.internal.options;

import java.util.Collections;
import java.util.Map;

import org.apache.flink.core.fs.Path;

/**
 * Validator for delta source and sink connector configuration options.
 *
 * Setting of an option is allowed for known option names. For invalid options, the validation
 * throws {@link DeltaOptionValidationException}. Known option names are passed via constructor
 * parameter {@code validOptions}.
 *
 * This is an internal class meant for connector implementations only.
 * Usage example (for sink):
 * <code>
 *     OptionValidator validator = new OptionValidator(sinkConfig, validSinkOptions);
 *     validator.option("mergeSchema", true);
 *     // For any option set on the sink, pass it to validator. If it's successful, sinkConfig
 *     // will be updated with the corresponding option.
 * </code>
 */
public class OptionValidator {
    private final Path tablePath;
    private final Map<String, DeltaConfigOption<?>> validOptions;
    private final DeltaConnectorConfiguration config;

    /**
     * Construct an option validator.
     *
     * @param tablePath Base path of the delta table.
     * @param config Configuration object that is populated with the validated options.
     * @param validOptions A map of valid options used by this instance.
     */
    public OptionValidator(
            Path tablePath,
            DeltaConnectorConfiguration config,
            Map<String, DeltaConfigOption<?>> validOptions) {
        this.tablePath = tablePath;
        this.config = config;
        this.validOptions = validOptions;
    }

    /**
     * Sets a configuration option.
     */
    public void option(String optionName, String optionValue) {
        tryToSetOption(() -> {
            DeltaConfigOption<?> configOption = validateOptionName(optionName);
            configOption.setOnConfig(config, optionValue);
        });
    }

    /**
     * Sets a configuration option.
     */
    public void option(String optionName, boolean optionValue) {
        tryToSetOption(() -> {
            DeltaConfigOption<?> configOption = validateOptionName(optionName);
            configOption.setOnConfig(config, optionValue);
        });
    }

    /**
     * Sets a configuration option.
     */
    public void option(String optionName, int optionValue) {
        tryToSetOption(() -> {
            DeltaConfigOption<?> configOption = validateOptionName(optionName);
            configOption.setOnConfig(config, optionValue);
        });
    }

    /**
     * Sets a configuration option.
     */
    public void option(String optionName, long optionValue) {
        tryToSetOption(() -> {
            DeltaConfigOption<?> configOption = validateOptionName(optionName);
            configOption.setOnConfig(config, optionValue);
        });
    }

    private void tryToSetOption(Executable argument) {
        try {
            argument.execute();
        } catch (Exception e) {
            throw optionValidationException(tablePath, e);
        }
    }

    @SuppressWarnings("unchecked")
    protected <TYPE> DeltaConfigOption<TYPE> validateOptionName(String optionName) {
        DeltaConfigOption<TYPE> option = (DeltaConfigOption<TYPE>) validOptions.get(optionName);
        if (option == null) {
            throw invalidOptionName(tablePath, optionName);
        }
        return option;
    }

    /** Exception to throw when the option name is invalid. */
    private static DeltaOptionValidationException invalidOptionName(
            Path tablePath,
            String invalidOption) {
        return new DeltaOptionValidationException(
            tablePath,
            Collections.singletonList(
                String.format("Invalid option [%s] used for Delta Connector.",
                    invalidOption)));
    }

    /** Exception to throw when there's an error while setting an option. */
    private static DeltaOptionValidationException optionValidationException(
            Path tablePath,
            Exception e) {
        return new DeltaOptionValidationException(
            tablePath,
            Collections.singletonList(e.getClass() + " - " + e.getMessage())
        );
    }

    @FunctionalInterface
    private interface Executable {
        void execute();
    }
}
