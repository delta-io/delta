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

import io.delta.kernel.data.Row;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import static io.delta.kernel.utils.Utils.requireNonNull;

public class Format
{
    public static Format fromRow(Row row)
    {
        if (row == null) {
            return null;
        }

        final String provider = requireNonNull(row, 0, "provider").getString(0);
        return new Format(provider);
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("provider", StringType.INSTANCE, false /* nullable */)
        .add("options",
            new MapType(StringType.INSTANCE, StringType.INSTANCE, false),
            true /* nullable */
        );

    private final String provider;

    public Format(String provider)
    {
        this.provider = provider;
    }
}
