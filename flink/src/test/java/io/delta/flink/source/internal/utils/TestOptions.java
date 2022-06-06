package io.delta.flink.source.internal.utils;

import java.util.UUID;

import io.delta.flink.source.internal.builder.DeltaConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class TestOptions {

    public static final DeltaConfigOption<Long> LONG_OPTION =
        DeltaConfigOption.of(
            ConfigOptions.key("longOption").longType().defaultValue(Long.MAX_VALUE),
            Long.class);

    public static final DeltaConfigOption<Integer> INT_OPTION =
        DeltaConfigOption.of(
            ConfigOptions.key("intOption").intType().defaultValue(Integer.MAX_VALUE),
            Integer.class);

    public static final DeltaConfigOption<String> STRING_OPTION =
        DeltaConfigOption.of(ConfigOptions.key("stringOption").stringType()
                .defaultValue(UUID.randomUUID().toString()),
            String.class);

    public static final DeltaConfigOption<Boolean> BOOLEAN_OPTION =
        DeltaConfigOption.of(
            ConfigOptions.key("booleanOption").booleanType().defaultValue(false),
            Boolean.class);

    public static final DeltaConfigOption<Boolean> NO_DEFAULT_VALUE =
        DeltaConfigOption.of(ConfigOptions.key("noDefault").booleanType().noDefaultValue(),
            Boolean.class);

}
