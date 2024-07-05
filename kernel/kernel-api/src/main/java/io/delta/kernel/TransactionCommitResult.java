/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;

/**
 * Contains the result of a successful transaction commit. Returned by
 * {@link Transaction#commit(Engine, CloseableIterable)}.
 *
 * @since 3.2.0
 */
@Evolving
public class TransactionCommitResult {
    private final long version;
    private final boolean isReadyForCheckpoint;

    public TransactionCommitResult(long version, boolean isReadyForCheckpoint) {
        this.version = version;
        this.isReadyForCheckpoint = isReadyForCheckpoint;
    }

    /**
     * Contains the version of the transaction committed as.
     *
     * @return version the transaction is committed as.
     */
    public long getVersion() {
        return version;
    }

    /**
     * Is the table ready for checkpoint (i.e. there are enough commits since the last checkpoint)?
     * If yes the connector can choose to checkpoint as the version the transaction is committed as
     * using {@link Table#checkpoint(Engine, long)}
     *
     * @return Is the table ready for checkpointing?
     */
    public boolean isReadyForCheckpoint() {
        return isReadyForCheckpoint;
    }
}

