/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    public SetTransaction(
            @Nonnull String appId,
            long version,
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
