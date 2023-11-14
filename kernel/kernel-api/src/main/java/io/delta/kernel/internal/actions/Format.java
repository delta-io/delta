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
package io.delta.kernel.internal.actions;

import java.util.Collections;
import java.util.Map;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.util.VectorUtils;
import static io.delta.kernel.internal.util.InternalUtils.requireNonNull;

public class Format {

    public static Format fromColumnVector(ColumnVector vector, int rowId) {
        if (vector.isNullAt(rowId)) {
            return null;
        }
        final String provider = requireNonNull(vector.getChild(0), rowId, "provider")
            .getString(rowId);
        final Map<String, String> options = vector.getChild(1).isNullAt(rowId) ?
            Collections.emptyMap() : VectorUtils.toJavaMap(vector.getChild(1).getMap(rowId));
        return new Format(provider, options);
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("provider", StringType.STRING, false /* nullable */)
        .add("options",
            new MapType(StringType.STRING, StringType.STRING, false),
            true /* nullable */
        );

    private final String provider;
    private final Map<String, String> options;

    public Format(String provider, Map<String, String> options) {
        this.provider = provider;
        this.options = options;
    }

    public String getProvider() {
        return provider;
    }

    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }
}
