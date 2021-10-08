/*
 * Copyright (2020) The Delta Lake Project Authors.
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

public class Protocol implements Action {
    private final int minReaderVersion;
    private final int minWriterVersion;

    public Protocol() {
        // TODO: have these statically and publicly defined
        this.minReaderVersion = 1;
        this.minWriterVersion = 2;
    }

    public Protocol(int minReaderVersion, int minWriterVersion) {
        this.minReaderVersion = minReaderVersion;
        this.minWriterVersion = minWriterVersion;
    }

    public int getMinReaderVersion() {
        return minReaderVersion;
    }

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

