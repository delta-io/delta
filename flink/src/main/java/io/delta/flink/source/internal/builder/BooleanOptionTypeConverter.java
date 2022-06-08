package io.delta.flink.source.internal.builder;

/**
 * Implementation of {@link OptionTypeConverter} that validates values for
 * {@link DeltaConfigOption} with type Boolean.
 */
public class BooleanOptionTypeConverter extends BaseOptionTypeConverter<Boolean> {

    /**
     * Converts String values for {@link DeltaConfigOption} with Boolean value type.
     * Strings "true" and "false" will be converted to Boolean true and false values.
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

        if (type == OptionType.BOOLEAN) {

            if ("true".equalsIgnoreCase(valueToConvert) ||
                "false".equalsIgnoreCase(valueToConvert)) {
                return (T) Boolean.valueOf(valueToConvert);
            }

            throw invalidValueException(desiredOption.key(), valueToConvert);
        }

        throw new IllegalArgumentException(
            String.format(
                "BooleanOptionTypeConverter used with a incompatible DeltaConfigOption "
                    + "option type. This converter must be used only for "
                    + "DeltaConfigOption::Boolean however it was used for '%s' with option '%s'",
                desiredOption.getValueType(), desiredOption.key())
        );
    }

    private IllegalArgumentException invalidValueException(
            String optionName,
            String valueToConvert) {
        return new IllegalArgumentException(
            String.format(
                "Illegal value used for [%s] option. Expected values "
                    + "\"true\" or \"false\" keywords (case insensitive) or boolean true,"
                    + " false values. Used value was [%s]",
                optionName, valueToConvert)
        );
    }
}
