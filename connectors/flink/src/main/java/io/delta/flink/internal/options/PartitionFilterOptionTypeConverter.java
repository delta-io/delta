package io.delta.flink.internal.options;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link OptionTypeConverter} that validates values for
 * {@link DeltaConfigOption} with type Map.
 */
public class PartitionFilterOptionTypeConverter extends BaseOptionTypeConverter<String> {

    /**
     * Converts String values for {@link DeltaConfigOption} with Map type.
     * E.g. "year=2023;month in (01,02),day=15". The expected format is a map where keys are
     * partition column names and values are sets of accepted values for given partition column.
     *
     * @param desiredOption  The {@link DeltaConfigOption} instance we want to do the conversion
     *                       for.
     * @param valueToConvert String value to convert.
     * @return A String representing Boolean value for given {@code valueToConvert} parameter.
     * @throws IllegalArgumentException in case of conversion failure.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T convertType(DeltaConfigOption<T> desiredOption, String valueToConvert) {
        Class<T> decoratedType = desiredOption.getValueType();
        OptionType type = OptionType.instanceFrom(decoratedType);

        if (type == OptionType.STRING) {

            try {
                parseStringToMap(valueToConvert);
                return (T) valueToConvert;
            } catch (Exception e) {
                throw invalidValueException(desiredOption.key(), valueToConvert);
            }
        }

        throw new IllegalArgumentException(
                String.format(
                        "PartitionFilterOptionTypeConverter used with a incompatible "
                                + "DeltaConfigOption option type. This converter must be used "
                                + " only for DeltaConfigOption::String however it was used for "
                                + "'%s' with option '%s'",
                        desiredOption.getValueType(), desiredOption.key())
        );
    }

    private IllegalArgumentException invalidValueException(
            String optionName,
            String valueToConvert) {
        return new IllegalArgumentException(
                String.format(
                        "Illegal value used for [%s] option. Expected values "
                                + "year=2023;month in (01,02);day=15. Used value was [%s]",
                        optionName, valueToConvert)
        );
    }

    public static Map<String, Set<String>> parseStringToMap(String input) {
        Map<String, Set<String>> map = new HashMap<>();
        if (input == null || input.trim().isEmpty()) {
            return map;
        }
        // Split the input string by semicolon to get individual key-value segments
        String[] segments = input.split(";");
        for (String segment : segments) {
            String key;
            Set<String> values = new HashSet<>();
            // Check if the segment contains " in (", indicating a set of values
            if (segment.contains(" in (")) {
                // Extract the key and the set of values
                int inIndex = segment.indexOf(" in (");
                key = segment.substring(0, inIndex).trim();
                String valuesString = segment.substring(inIndex + 5, segment.length() - 1);
                String[] individualValues = valuesString.split(",");
                for (String value : individualValues) {
                    values.add(value.trim());
                }
            } else {
                // Handle single key-value pairs
                String[] keyValue = segment.split("=");
                key = keyValue[0].trim();
                values.add(keyValue[1].trim());
            }
            // Add the key and its set of values to the map
            map.put(key, values);
        }
        return map;
    }
}
