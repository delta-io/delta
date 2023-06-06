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

import java.util.Objects;

/**
 * Used to block older clients from reading or writing the log when backwards
 * incompatible changes are made to the protocol. Readers and writers are
 * responsible for checking that they meet the minimum versions before performing
 * any other operations.
 * <p>
 * Since this action allows us to explicitly block older clients in the case of a
 * breaking change to the protocol, clients should be tolerant of messages and
 * fields that they do not understand.
 *
 * @see  <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#protocol-evolution">Delta Transaction Log Protocol: Protocol Evolution</a>
 */
public final class Protocol implements Action {
    private final int minReaderVersion;
    private final int minWriterVersion;

    public Protocol(int minReaderVersion, int minWriterVersion) {
        this.minReaderVersion = minReaderVersion;
        this.minWriterVersion = minWriterVersion;
    }

    /**
     * @return the minimum version of the Delta read protocol that a client must implement in order
     *         to correctly read this table
     */
    public int getMinReaderVersion() {
        return minReaderVersion;
    }

    /**
     * @return the minimum version of the Delta write protocol that a client must implement in order
     *         to correctly write this table
     */
    public int getMinWriterVersion() {
        return minWriterVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Protocol protocol = (Protocol) o;
        return minReaderVersion == protocol.minReaderVersion &&
            minWriterVersion == protocol.minWriterVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minReaderVersion, minWriterVersion);
    }
}

