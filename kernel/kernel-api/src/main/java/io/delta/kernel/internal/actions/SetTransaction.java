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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.data.GenericRow;

/**
 * Delta log action representing a transaction identifier action.
 */
public class SetTransaction {
    public static final StructType FULL_SCHEMA = new StructType()
        .add("appId", StringType.STRING, false /* nullable */)
        .add("version", LongType.LONG, false /* nullable*/)
        .add("lastUpdated", LongType.LONG, true /* nullable*/);

    public static SetTransaction fromColumnVector(ColumnVector vector, int rowId) {
        if (vector.isNullAt(rowId)) {
            return null;
        }
        return new SetTransaction(
            vector.getChild(0).getString(rowId),
            vector.getChild(1).getLong(rowId),
            vector.getChild(2).isNullAt(rowId) ?
                Optional.empty() : Optional.of(vector.getChild(2).getLong(rowId)));
    }

    private final String appId;
    private final long version;
    private final Optional<Long> lastUpdated;

    public SetTransaction(String appId, Long version, Optional<Long> lastUpdated) {
        this.appId = appId;
        this.version = version;
        this.lastUpdated = lastUpdated;
    }

    public String getAppId() {
        return appId;
    }

    public long getVersion() {
        return version;
    }

    public Optional<Long> getLastUpdated() {
        return lastUpdated;
    }

    /**
     * Encode as a {@link Row} object with the schema {@link SetTransaction#FULL_SCHEMA}.
     *
     * @return {@link Row} object with the schema {@link SetTransaction#FULL_SCHEMA}
     */
    public Row toRow() {
        Map<Integer, Object> setTransactionMap = new HashMap<>();
        setTransactionMap.put(0, appId);
        setTransactionMap.put(1, version);
        setTransactionMap.put(2, lastUpdated.orElse(null));

        return new GenericRow(SetTransaction.FULL_SCHEMA, setTransactionMap);
    }
}
