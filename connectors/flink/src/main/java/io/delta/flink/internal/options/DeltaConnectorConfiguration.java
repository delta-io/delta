package io.delta.flink.internal.options;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.delta.flink.source.internal.DeltaSourceOptions;
import org.apache.flink.configuration.ConfigOption;

/**
 * This class keeps options used for delta source and sink connectors.
 *
 * @implNote This class should not be used directly by user but rather indirectly through source or
 * sink builders which will have dedicated setter methods for public options.
 */
public class DeltaConnectorConfiguration implements Serializable {

    /**
     * Map of used Options. The map entry key is a string representation of used option name
     * and the entry map value is equal option's value used for this entry.
     *
     * @implNote The {@code DeltaConnectorConfiguration} object will be de/serialized by flink and
     * passed to Cluster node during job initialization. For that the map content has to be
     * serializable as well. The {@link ConfigOption} is not a serializable object, and therefore it
     * cannot be used as a map entry key.
     */
    private final Map<String, Object> usedSourceOptions = new HashMap<>();

    /**
     * Creates {@link DeltaConnectorConfiguration} instance without any options.
     */
    public DeltaConnectorConfiguration() {

    }

    /**
     * Creates a copy of DeltaSourceConfiguration. Changes to the copy object do not influence
     * the state of the original object.
     */
    public DeltaConnectorConfiguration copy() {
        return new DeltaConnectorConfiguration(this.usedSourceOptions);
    }

    /**
     * Creates an instance of {@link DeltaConnectorConfiguration} using provided options.
     * @param options options that should be added to {@link DeltaConnectorConfiguration}.
     */
    public DeltaConnectorConfiguration(Map<String, Object> options) {
        this.usedSourceOptions.putAll(options);
    }

    public <T> DeltaConnectorConfiguration addOption(DeltaConfigOption<T> name, T value) {
        this.usedSourceOptions.put(name.key(), value);
        return this;
    }

    public boolean hasOption(DeltaConfigOption<?> option) {
        return this.usedSourceOptions.containsKey(option.key());
    }

    public Set<String> getUsedOptions() {
        return this.usedSourceOptions.keySet();
    }

    /**
     * This method returns a value for used {@code DeltaSourceOption}. The type of returned value
     * will be cast to the same type that was used in {@link DeltaSourceOptions} definition.
     * Using {@code DeltaSourceOption} object as an argument rather than option's string key
     * guaranties type safety.
     *
     * @param option The {@code DeltaSourceOption} for which we want to get the value.
     * @param <T>    Type of returned value. It will be same type used in {@link DeltaSourceOptions}
     *               definition.
     * @return A value for given option if used or a default value if defined or null if none.
     */
    @SuppressWarnings("unchecked")
    public <T> T getValue(DeltaConfigOption<T> option) {
        return (T) getValue(option.key()).orElse(option.defaultValue());
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<T> getValue(String optionName) {
        return (Optional<T>) Optional.ofNullable(this.usedSourceOptions.get(optionName));
    }

    @Override
    public String toString() {
        return "DeltaSourceConfiguration{" +
            "usedSourceOptions=" + usedSourceOptions +
            '}';
    }
}
