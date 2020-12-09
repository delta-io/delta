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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A specification of the encoding for the files stored in a table.
 *
 * @see  <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md">Delta Transaction Log Protocol</a>
 */
public final class Format {
    private final String provider;
    private final Map<String, String> options;

    public Format(String provider, Map<String, String> options) {
        this.provider = provider;
        this.options = options;
    }

    /**
     * @return the name of the encoding for files in this table
     */
    public String getProvider() {
        return provider;
    }

    /**
     * @return an unmodifiable {@code Map} containing configuration options for
     *         the format
     */
    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Format format = (Format) o;
        return Objects.equals(provider, format.provider) &&
                Objects.equals(options, format.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(provider, options);
    }
}
