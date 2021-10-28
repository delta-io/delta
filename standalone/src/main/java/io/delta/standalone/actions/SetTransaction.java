// TODO: copyright

package io.delta.standalone.actions;

import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Sets the committed version for a given application. Used to make operations like streaming append
 * idempotent.
 */
public final class SetTransaction implements Action {
    @Nonnull
    private final String appId;

    private final long version;

    @Nonnull
    private final Optional<Long> lastUpdated;

    public SetTransaction(@Nonnull String appId, long version,
                          @Nonnull Optional<Long> lastUpdated) {
        this.appId = appId;
        this.version = version;
        this.lastUpdated = lastUpdated;
    }

    /**
     * @return the application ID
     */
    @Nonnull
    public String getAppId() {
        return appId;
    }

    /**
     * @return the committed version for the application ID
     */
    public long getVersion() {
        return version;
    }

    /**
     * @return the last updated timestamp of this transaction (milliseconds since the epoch)
     */
    @Nonnull
    public Optional<Long> getLastUpdated() {
        return lastUpdated;
    }
}
