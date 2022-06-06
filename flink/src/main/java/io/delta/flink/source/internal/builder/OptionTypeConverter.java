package io.delta.flink.source.internal.builder;

public interface OptionTypeConverter {

    <T> T convertType(DeltaConfigOption<T> desiredOption, Integer valueToConvert);

    <T> T convertType(DeltaConfigOption<T> desiredOption, Long valueToConvert);

    <T> T convertType(DeltaConfigOption<T> desiredOption, Boolean valueToConvert);

    <T> T convertType(DeltaConfigOption<T> desiredOption, String valueToConvert);
}
