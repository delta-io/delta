package io.delta.flink.source.internal.builder;

import org.apache.flink.util.StringUtils;

/**
 * Implementation of {@link OptionTypeConverter} that validates values for
 * {@link DeltaConfigOption} with type {@code <? extends Number>}. Allowed types are {@link Integer}
 * and {@link Long}
 */
public class NonNegativeNumberTypeConverter<TYPE extends Number>
    extends BaseOptionTypeConverter<TYPE> {

    /**
     * Validates value of {@link DeltaConfigOption} which value should represent a non-negative
     * integer value.
     * <p>
     *
     * @param desiredOption  The {@link DeltaConfigOption} instance we want to do the conversion
     *                       for.
     * @param valueToConvert String value to validate.
     * @return A String representing a non-negative integer.
     * @throws IllegalArgumentException in case of validation failure.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T convertType(DeltaConfigOption<T> desiredOption, String valueToConvert) {
        Class<T> decoratedType = desiredOption.getValueType();
        OptionType type = OptionType.instanceFrom(decoratedType);

        if (StringUtils.isNullOrWhitespaceOnly(valueToConvert)) {
            throw invalidValueException(desiredOption.key(), valueToConvert);
        }

        Number convertedValue;

        switch (type) {
            case LONG:
                convertedValue = Long.parseLong(valueToConvert);
                break;
            case INTEGER:
                convertedValue = Integer.parseInt(valueToConvert);
                break;
            default:
                throw new IllegalArgumentException(
                    String.format(
                        "NonNegativeNumberTypeConverter used with a incompatible DeltaConfigOption "
                            + "option type. This converter must be used only for "
                            + "DeltaConfigOption::<? extends Number> however it was used for '%s'"
                            + " with option '%s'", desiredOption.getValueType(), desiredOption.key()
                    )
                );
        }

        if (convertedValue.longValue() >= 0) {
            return (T) convertedValue;
        } else {
            throw invalidValueException(desiredOption.key(), valueToConvert);
        }
    }

    private IllegalArgumentException invalidValueException(
            String optionName,
            String valueToConvert) {
        return new IllegalArgumentException(
            String.format(
                "Illegal value used for [%s] option. Expected values "
                    + "are non-negative integers. Used value was [%s]", optionName, valueToConvert)
        );
    }
}
