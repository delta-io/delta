package io.delta.flink.source.internal.builder;

public abstract class BaseOptionTypeConverter<TYPE>
    implements OptionTypeConverter<TYPE> {

    /**
     * Converts an Integer valueToConvert to desired type of
     * {@link DeltaConfigOption#getValueType()}.
     *
     * @param desiredOption  A {@link DeltaConfigOption} to which type the valueToConvert parameter
     *                       should be converted.
     * @param valueToConvert A valueToConvert that type should be converted.
     * @param <T>            A type to which "valueToConvert" parameter will be converted to.
     * @return valueToConvert with converted type to {@link DeltaConfigOption#getValueType()}.
     */
    @Override
    public <T> T convertType(DeltaConfigOption<T> desiredOption, Integer valueToConvert) {
        return convertType(desiredOption, String.valueOf(valueToConvert));
    }

    /**
     * Converts a Long valueToConvert to desired type of {@link DeltaConfigOption#getValueType()}.
     *
     * @param desiredOption  A {@link DeltaConfigOption} to which type the valueToConvert parameter
     *                       should be converted.
     * @param valueToConvert A valueToConvert that type should be converted.
     * @param <T>            A type to which "valueToConvert" parameter will be converted to.
     * @return valueToConvert with converted type to {@link DeltaConfigOption#getValueType()}.
     */
    @Override
    public <T> T convertType(DeltaConfigOption<T> desiredOption, Long valueToConvert) {
        return convertType(desiredOption, String.valueOf(valueToConvert));
    }

    /**
     * Converts a Boolean valueToConvert to desired type of
     * {@link DeltaConfigOption#getValueType()}.
     *
     * @param desiredOption  A {@link DeltaConfigOption} to which type the valueToConvert parameter
     *                       should be converted.
     * @param valueToConvert A valueToConvert that type should be converted.
     * @param <T>            A type to which "valueToConvert" parameter will be converted to.
     * @return valueToConvert with converted type to {@link DeltaConfigOption#getValueType()}.
     */
    @Override
    public <T> T convertType(DeltaConfigOption<T> desiredOption, Boolean valueToConvert) {
        return convertType(desiredOption, String.valueOf(valueToConvert));
    }
}
