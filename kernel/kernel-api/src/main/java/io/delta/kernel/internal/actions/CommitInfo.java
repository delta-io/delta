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

import java.util.HashMap;
import java.util.Map;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.data.GenericRow;

/**
 * Delta log action representing a commit information action. According to the Delta protocol
 * there isn't any specific schema for this action, but we use the following schema:
 * <ul>
 *     <li>timestamp: Long - Milliseconds since epoch UTC of when this commit happened</li>
 *     <li>engineInfo: String - Engine that made this commit</li>
 *     <li>operation: String - Operation (e.g. insert, delete, merge etc.)</li>
 * </ul>
 * The Delta-Spark connector adds lot more fields to this action. We can add them as needed.
 */
public class CommitInfo {

    public static StructType FULL_SCHEMA = new StructType()
            .add("timestamp", LongType.LONG)
            .add("engineInfo", StringType.STRING)
            .add("operation", StringType.STRING);

    private final long timestamp;
    private final String engineInfo;
    private final String operation;

    public CommitInfo(long timestamp, String engineInfo, String operation) {
        this.timestamp = timestamp;
        this.engineInfo = engineInfo;
        this.operation = operation;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getEngineInfo() {
        return engineInfo;
    }

    public String getOperation() {
        return operation;
    }

    /**
     * Encode as a {@link Row} object with the schema {@link CommitInfo#FULL_SCHEMA}.
     *
     * @return {@link Row} object with the schema {@link CommitInfo#FULL_SCHEMA}
     */
    public Row toRow() {
        Map<Integer, Object> commitInfo = new HashMap<>();
        commitInfo.put(0, timestamp);
        commitInfo.put(1, engineInfo);
        commitInfo.put(2, operation);

        return new GenericRow(CommitInfo.FULL_SCHEMA, commitInfo);
    }
}
