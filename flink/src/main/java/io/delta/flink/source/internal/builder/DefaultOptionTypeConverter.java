package io.delta.flink.source.internal.builder;

/**
 * An implementation of {@link OptionTypeConverter} interface to convert {@link DeltaConfigOption}
 * values to desired {@link Class} type.
 */
public final class DefaultOptionTypeConverter extends BaseOptionTypeConverter {

    private static final String TYPE_EXCEPTION_MSG = "Unsupported value type {%s] for option [%s]";

    /**
     * Converts a String valueToConvert to desired type of {@link DeltaConfigOption#getValueType()}.
     *
     * @param desiredOption  A {@link DeltaConfigOption} to which type the valueToConvert parameter
     *                       should be converted.
     * @param valueToConvert A valueToConvert that type should be converted.
     * @param <T>            A type to which "valueToConvert" parameter will be converted to.
     * @return valueToConvert with converted type to {@link DeltaConfigOption#getValueType()}.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T convertType(DeltaConfigOption<T> desiredOption, String valueToConvert) {
        Class<T> decoratedType = desiredOption.getValueType();
        OptionType type = OptionType.instanceFrom(decoratedType);
        switch (type) {
            case STRING:
                return (T) valueToConvert;
            case BOOLEAN:
                return (T) Boolean.valueOf(valueToConvert);
            case INTEGER:
                return (T) Integer.valueOf(valueToConvert);
            case LONG:
                return (T) Long.valueOf(valueToConvert);
            case OTHER:
            default:
                throw new IllegalArgumentException(
                    String.format(TYPE_EXCEPTION_MSG, decoratedType, desiredOption.key())
                );
        }
    }
}
