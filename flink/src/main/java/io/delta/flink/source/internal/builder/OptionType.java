package io.delta.flink.source.internal.builder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An Enum supported Java types for {@link io.delta.flink.source.internal.DeltaSourceOptions}.
 *
 * <p>
 * This Enum can be used for example to build switch statement based on {@link Class} type.
 */
public enum OptionType {
    STRING(String.class),
    BOOLEAN(Boolean.class),
    INTEGER(Integer.class),
    LONG(Long.class),
    OTHER(null);

    private static final Map<Class<?>, OptionType> LOOKUP_MAP;

    static {
        Map<Class<?>, OptionType> tmpMap = new HashMap<>();
        for (OptionType type : OptionType.values()) {
            tmpMap.put(type.optionType, type);
        }
        LOOKUP_MAP = Collections.unmodifiableMap(tmpMap);
    }

    private final Class<?> optionType;

    OptionType(Class<?> optionType) {
        this.optionType = optionType;
    }

    /**
     * @param optionType A desired Java {@link Class} type
     * @return mapped instance of {@link OptionType} Enum.
     */
    public static OptionType instanceFrom(Class<?> optionType) {
        return LOOKUP_MAP.getOrDefault(optionType, OTHER);
    }
}
