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

import java.util.*;
import static java.util.Collections.emptyMap;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.data.GenericRow;
import static io.delta.kernel.internal.util.InternalUtils.requireNonNull;
import static io.delta.kernel.internal.util.VectorUtils.*;

public class Format {

    public static Format fromColumnVector(ColumnVector vector, int rowId) {
        if (vector.isNullAt(rowId)) {
            return null;
        }
        final String provider = requireNonNull(vector.getChild(0), rowId, "provider")
            .getString(rowId);
        final Map<String, String> options = vector.getChild(1).isNullAt(rowId) ?
            Collections.emptyMap() : toJavaMap(vector.getChild(1).getMap(rowId));
        return new Format(provider, options);
    }

    public static final StructType FULL_SCHEMA = new StructType()
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

    public Format() {
        this.provider = "parquet";
        this.options = emptyMap();
    }

    public String getProvider() {
        return provider;
    }

    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }

    /**
     * Encode as a {@link Row} object with the schema {@link Format#FULL_SCHEMA}.
     *
     * @return {@link Row} object with the schema {@link Format#FULL_SCHEMA}
     */
    public Row toRow() {
        Map<Integer, Object> formatMap = new HashMap<>();
        formatMap.put(0, provider);
        formatMap.put(1, stringStringMapValue(options));

        return new GenericRow(Format.FULL_SCHEMA, formatMap);
    }
}
