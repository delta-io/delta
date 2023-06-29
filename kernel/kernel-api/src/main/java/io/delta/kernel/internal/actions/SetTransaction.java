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
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import static io.delta.kernel.utils.Utils.requireNonNull;

public class SetTransaction
    implements Action
{
    public static SetTransaction fromRow(Row row)
    {
        if (row == null) {
            return null;
        }

        return new SetTransaction(
            requireNonNull(row, 0, "appId").getString(0),
            requireNonNull(row, 1, "version").getLong(1),
            requireNonNull(row, 2, "lastUpdated").getLong(2)
        );
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("appId", StringType.INSTANCE)
        .add("version", LongType.INSTANCE)
        .add("lastUpdated", LongType.INSTANCE);

    private final String appId;
    private final long version;
    private final long lastUpdated;

    public SetTransaction(
        String appId,
        long version,
        long lastUpdated)
    {
        this.appId = appId;
        this.version = version;
        this.lastUpdated = lastUpdated;
    }

    public String getAppId()
    {
        return appId;
    }

    public long getVersion()
    {
        return version;
    }

    public long getLastUpdated()
    {
        return lastUpdated;
    }
}
