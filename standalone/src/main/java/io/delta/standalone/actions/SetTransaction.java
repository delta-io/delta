package io.delta.standalone.actions;

import java.util.Optional;

public class SetTransaction implements Action {
    private final String appId;
    private final long verion;
    private final Optional<Long> lastUpdated;

    public SetTransaction(String appId, long verion, Optional<Long> lastUpdated) {
        this.appId = appId;
        this.verion = verion;
        this.lastUpdated = lastUpdated;
    }

    public String getAppId() {
        return appId;
    }

    public long getVerion() {
        return verion;
    }

    public Optional<Long> getLastUpdated() {
        return lastUpdated;
    }
}
