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

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;
import static java.util.stream.Collectors.toMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.InvalidTableException;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.VectorUtils;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;

/**
 * Delta log action representing a commit information action. According to the Delta protocol there
 * isn't any specific schema for this action, but we use the following schema:
 * <ul>
 *     <li>inCommitTimestamp: Long - A monotonically increasing timestamp that represents the time
 *     since epoch in milliseconds when the commit write was started</li>
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

    public static CommitInfo fromColumnVector(ColumnVector vector, int rowId) {
        if (vector.isNullAt(rowId)) {
            return null;
        }

        ColumnVector[] children = new ColumnVector[7];
        for (int i = 0; i < 7; i++) {
            children[i] = vector.getChild(i);
        }

        return new CommitInfo(
                Optional.ofNullable(children[0].isNullAt(rowId) ? null :
                        children[0].getLong(rowId)),
                children[1].isNullAt(rowId) ? null : children[1].getLong(rowId),
                children[2].isNullAt(rowId) ? null : children[2].getString(rowId),
                children[3].isNullAt(rowId) ? null : children[3].getString(rowId),
                children[4].isNullAt(rowId) ? Collections.emptyMap() :
                        VectorUtils.toJavaMap(children[4].getMap(rowId)),
                children[5].isNullAt(rowId) ? null : children[5].getBoolean(rowId),
                children[6].isNullAt(rowId) ? null : children[6].getString(rowId)
        );
    }

    public static StructType FULL_SCHEMA = new StructType()
            .add("inCommitTimestamp", LongType.LONG, true /* nullable */)
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

    private static final Logger logger = LoggerFactory.getLogger(CommitInfo.class);

    private final long timestamp;
    private final String engineInfo;
    private final String operation;
    private final Map<String, String> operationParameters;
    private final boolean isBlindAppend;
    private final String txnId;
    private Optional<Long> inCommitTimestamp;

    public static CommitInfo empty() {
        return new CommitInfo(
                Optional.empty(),
                -1,
                null,
                null,
                Collections.emptyMap(),
                true,
                null);
    }

    public CommitInfo(
            Optional<Long> inCommitTimestamp,
            long timestamp,
            String engineInfo,
            String operation,
            Map<String, String> operationParameters,
            boolean isBlindAppend,
            String txnId) {
        this.inCommitTimestamp = inCommitTimestamp;
        this.timestamp = timestamp;
        this.engineInfo = engineInfo;
        this.operation = operation;
        this.operationParameters = Collections.unmodifiableMap(operationParameters);
        this.isBlindAppend = isBlindAppend;
        this.txnId = txnId;
    }

    public CommitInfo withTimestamp(long newTimestamp) {
        return new CommitInfo(
                Optional.of(newTimestamp),
                newTimestamp,
                this.engineInfo,
                this.operation,
                this.operationParameters,
                this.isBlindAppend,
                this.txnId
        );
    }

    public Optional<Long> getInCommitTimestamp() {
        return inCommitTimestamp;
    }

    public void setInCommitTimestamp(Optional<Long> inCommitTimestamp) {
        this.inCommitTimestamp = inCommitTimestamp;
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
        commitInfo.put(COL_NAME_TO_ORDINAL.get("inCommitTimestamp"),
                inCommitTimestamp.orElse(null));
        commitInfo.put(COL_NAME_TO_ORDINAL.get("timestamp"), timestamp);
        commitInfo.put(COL_NAME_TO_ORDINAL.get("engineInfo"), engineInfo);
        commitInfo.put(COL_NAME_TO_ORDINAL.get("operation"), operation);
        commitInfo.put(COL_NAME_TO_ORDINAL.get("operationParameters"),
                stringStringMapValue(operationParameters));
        commitInfo.put(COL_NAME_TO_ORDINAL.get("isBlindAppend"), isBlindAppend);
        commitInfo.put(COL_NAME_TO_ORDINAL.get("txnId"), txnId);

        return new GenericRow(CommitInfo.FULL_SCHEMA, commitInfo);
    }

    /**
     * Returns the `inCommitTimestamp` of the given `commitInfoOpt` if it is defined.
     * Throws an exception if `commitInfoOpt` is empty or contains an empty `inCommitTimestamp`.
     */
    public static long getRequiredInCommitTimestamp(
            Optional<CommitInfo> commitInfoOpt, String version, Path dataPath) {
        CommitInfo commitInfo = commitInfoOpt
                .orElseThrow(() -> new InvalidTableException(
                        dataPath.toString(),
                        String.format("This table has the feature inCommitTimestamp-preview " +
                                "enabled which requires the presence of the CommitInfo action " +
                                "in every commit. However, the CommitInfo action is " +
                                "missing from commit version %s.", version)));
        return commitInfo
                .inCommitTimestamp
                .orElseThrow(() -> new InvalidTableException(
                        dataPath.toString(),
                        String.format("This table has the feature inCommitTimestamp-preview " +
                                "enabled which requires the presence of inCommitTimestamp in the " +
                                "CommitInfo action. However, this field has not " +
                                "been set in commit version %s.", version)));
    }

    /** Get the persisted commit info (if available) for the given delta file. */
    public static Optional<CommitInfo> getCommitInfoOpt(
            Engine engine,
            Path logPath,
            long version) throws IOException {
        final FileStatus file = FileStatus.of(
                FileNames.deltaFile(logPath, version), /* path */
                0, /* size */
                0 /* modification time */);
        final StructType COMMITINFO_READ_SCHEMA = new StructType()
                .add("commitInfo", CommitInfo.FULL_SCHEMA);
        try (CloseableIterator<ColumnarBatch> columnarBatchIter = engine.getJsonHandler()
                .readJsonFiles(
                        singletonCloseableIterator(file),
                        COMMITINFO_READ_SCHEMA,
                        Optional.empty())) {
            while (columnarBatchIter.hasNext()) {
                final ColumnarBatch columnarBatch = columnarBatchIter.next();
                assert(columnarBatch.getSchema().equals(COMMITINFO_READ_SCHEMA));
                final ColumnVector commitInfoVector = columnarBatch.getColumnVector(0);
                for (int i = 0; i < commitInfoVector.getSize(); i++) {
                    if (!commitInfoVector.isNullAt(i)) {
                        CommitInfo commitInfo = CommitInfo.fromColumnVector(commitInfoVector, i);
                        if (commitInfo != null) {
                            return Optional.of(commitInfo);
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Could not close iterator", ex);
        }

        logger.info("No commit info found for commit of version {}", version);
        return Optional.empty();
    }
}
