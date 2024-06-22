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
import java.util.stream.IntStream;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.data.GenericRow;
import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;

/**
 * Delta log action representing a commit information action. According to the Delta protocol there
 * isn't any specific schema for this action, but we use the following schema:
 * <ul>
 *     <li>timestamp: Long - Milliseconds since epoch UTC of when this commit happened</li>
 *     <li>engineInfo: String - Engine that made this commit</li>
 *     <li>operation: String - Operation (e.g. insert, delete, merge etc.)</li>
 *     <li>operationParameters: Map(String, String) - each operation depending upon the type
 *     may add zero or more parameters about the operation. E.g. when creating a table
 *     `partitionBy` key with list of partition columns is added.</li>
 *     <li>isBlindAppend: Boolean - Is this commit a blind append?</li>
 *     <li>txnId: String - a unique transaction id of this commit</li>
 * </ul>
 * The Delta-Spark connector adds lot more fields to this action. We can add them as needed.
 */
public class CommitInfo {

    public static StructType FULL_SCHEMA = new StructType()
            .add("timestamp", LongType.LONG)
            .add("engineInfo", StringType.STRING)
            .add("operation", StringType.STRING)
            .add("operationParameters",
                    new MapType(StringType.STRING, StringType.STRING, true /* nullable */))
            .add("isBlindAppend", BooleanType.BOOLEAN, true /* nullable */)
            .add("txnId", StringType.STRING);

    private static final Map<String, Integer> COL_NAME_TO_ORDINAL =
            IntStream.range(0, FULL_SCHEMA.length())
                    .boxed()
                    .collect(toMap(i -> FULL_SCHEMA.at(i).getName(), i -> i));

    private final long timestamp;
    private final String engineInfo;
    private final String operation;
    private final Map<String, String> operationParameters;
    private final boolean isBlindAppend;
    private final String txnId;

    public CommitInfo(
            long timestamp,
            String engineInfo,
            String operation,
            Map<String, String> operationParameters,
            boolean isBlindAppend,
            String txnId) {
        this.timestamp = timestamp;
        this.engineInfo = engineInfo;
        this.operation = operation;
        this.operationParameters = Collections.unmodifiableMap(operationParameters);
        this.isBlindAppend = isBlindAppend;
        this.txnId = txnId;
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
        commitInfo.put(COL_NAME_TO_ORDINAL.get("timestamp"), timestamp);
        commitInfo.put(COL_NAME_TO_ORDINAL.get("engineInfo"), engineInfo);
        commitInfo.put(COL_NAME_TO_ORDINAL.get("operation"), operation);
        commitInfo.put(COL_NAME_TO_ORDINAL.get("operationParameters"),
                stringStringMapValue(operationParameters));
        commitInfo.put(COL_NAME_TO_ORDINAL.get("isBlindAppend"), isBlindAppend);
        commitInfo.put(COL_NAME_TO_ORDINAL.get("txnId"), txnId);

        return new GenericRow(CommitInfo.FULL_SCHEMA, commitInfo);
    }
}
