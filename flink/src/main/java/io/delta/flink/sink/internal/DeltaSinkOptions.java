package io.delta.flink.sink.internal;

import java.util.HashMap;
import java.util.Map;

import io.delta.flink.internal.options.DeltaConfigOption;

/**
 * This class contains all available options for {@link io.delta.flink.sink.DeltaSink} with
 * their type and default values.
 */
public class DeltaSinkOptions {
    /**
     * A map of all valid {@code DeltaSinkOptions}. This map can be used for example by {@code
     * RowDataDeltaSinkBuilder} to do configuration sanity check.
     */
    public static final Map<String, DeltaConfigOption<?>> USER_FACING_SINK_OPTIONS =
        new HashMap<>();

    /**
     * A map of all {@code DeltaSinkOptions} that are internal only, meaning that they must not be
     * used by end user through public API. This map can be used for example by {@code
     * RowDataDeltaSinkBuilder} to do configuration sanity check.
     */
    public static final Map<String, DeltaConfigOption<?>> INNER_SINK_OPTIONS = new HashMap<>();
}
