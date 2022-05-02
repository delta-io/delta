package io.delta.flink.source.internal.exceptions;

import java.util.Optional;

/**
 * A runtime exception throw by {@link io.delta.flink.source.DeltaSource} components.
 */
public class DeltaSourceException extends RuntimeException {

    /**
     * Path to Delta table for which exception was thrown.
     */
    private final String tablePath;

    /**
     * The {@link io.delta.standalone.Snapshot} version for which exception was throw.
     * <p>
     * This value can be null, meaning that we were not able to identify snapshot version for this
     * exception.
     */
    private final Long snapshotVersion;

    public DeltaSourceException(String tablePath, Long snapshotVersion, Throwable cause) {
        super(cause);
        this.tablePath = tablePath;
        this.snapshotVersion = snapshotVersion;
    }

    public DeltaSourceException(String tablePath, Long snapshotVersion, String message) {
        super(message);
        this.tablePath = tablePath;
        this.snapshotVersion = snapshotVersion;
    }

    public DeltaSourceException(String tablePath, Long snapshotVersion, String message,
        Throwable cause) {
        super(message, cause);
        this.tablePath = tablePath;
        this.snapshotVersion = snapshotVersion;
    }

    /**
     * @return Delta table path for which this exception was thrown.
     */
    public String getTablePath() {
        return tablePath;
    }

    /**
     * @return An {@link Optional} value with {@link io.delta.standalone.Snapshot} version for which
     * this exception was thrown. If snapshot value was unknown, then the returned optional will be
     * empty.
     */
    public Optional<Long> getSnapshotVersion() {
        return Optional.ofNullable(snapshotVersion);
    }
}
