package io.delta.flink.sink.internal.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple logging interface providing common utility logging methods
 */
public interface Logging {

    default Logger getLogger() {
        return LoggerFactory.getLogger(this.getClass());
    }

    default void logInfo(String message) {
        getLogger().info(message);
    }

    default boolean isDebugEnabled() {
        return getLogger().isDebugEnabled();
    }

    default void logDebug(String format, Object obj) {
        getLogger().debug(format, obj);
    }

    default void logDebug(String format, Object arg1, Object arg2) {
        getLogger().debug(format, arg1, arg2);
    }

    default void logDebug(String format, Object... arguments) {
        getLogger().debug(format, arguments);
    }
}
