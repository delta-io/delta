package io.delta.flink.source.internal.builder;

/**
 * Converter and validator for {@link DeltaConfigOption} values.
 *
 * @param <TYPE> A type of {@link DeltaConfigOption} on which this converter can be used. The {@code
 *               <TYPE>} must match {@link DeltaConfigOption#getValueType()}
 */
public interface OptionTypeConverter<TYPE> {

    <T> T convertType(DeltaConfigOption<T> desiredOption, Integer valueToConvert);

    <T> T convertType(DeltaConfigOption<T> desiredOption, Long valueToConvert);

    <T> T convertType(DeltaConfigOption<T> desiredOption, Boolean valueToConvert);

    <T> T convertType(DeltaConfigOption<T> desiredOption, String valueToConvert);
}
