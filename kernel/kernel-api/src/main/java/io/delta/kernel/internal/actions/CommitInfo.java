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

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractCommitInfo;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delta log action representing a commit information action. According to the Delta protocol there
 * isn't any specific schema for this action, but we use the following schema:
 *
 * <ul>
 *   <li>inCommitTimestamp: Long - A monotonically increasing timestamp that represents the time
 *       since epoch in milliseconds when the commit write was started
 *   <li>timestamp: Long - Milliseconds since epoch UTC of when this commit happened
 *   <li>engineInfo: String - Engine that made this commit
 *   <li>operation: String - Operation (e.g. insert, delete, merge etc.)
 *   <li>operationParameters: Map(String, String) - each operation depending upon the type may add
 *       zero or more parameters about the operation. E.g. when creating a table `partitionBy` key
 *       with list of partition columns is added.
 *   <li>isBlindAppend: Boolean - Is this commit a blind append?
 *   <li>txnId: String - a unique transaction id of this commit
 * </ul>
 *
 * The Delta-Spark connector adds lot more fields to this action. We can add them as needed.
 */
public class CommitInfo implements AbstractCommitInfo {

  ///////////////////
  // Static Fields //
  ///////////////////

  public static StructType FULL_SCHEMA =
      new StructType()
          .add("inCommitTimestamp", LongType.LONG, true /* nullable */)
          .add("timestamp", LongType.LONG)
          .add("engineInfo", StringType.STRING)
          .add("operation", StringType.STRING)
          .add(
              "operationParameters",
              new MapType(StringType.STRING, StringType.STRING, true /* nullable */))
          .add("isBlindAppend", BooleanType.BOOLEAN, true /* nullable */)
          .add("txnId", StringType.STRING)
          .add(
              "operationMetrics",
              new MapType(StringType.STRING, StringType.STRING, true /* nullable */));

  private static final Map<String, Integer> COL_NAME_TO_ORDINAL =
      IntStream.range(0, FULL_SCHEMA.length())
          .boxed()
          .collect(toMap(i -> FULL_SCHEMA.at(i).getName(), i -> i));

  private static final Logger logger = LoggerFactory.getLogger(CommitInfo.class);

  ////////////////////
  // Static Methods //
  ////////////////////

  public static CommitInfo fromColumnVector(ColumnVector vector, int rowId, long version) {
    if (vector.isNullAt(rowId)) {
      return null;
    }

    ColumnVector[] children = new ColumnVector[FULL_SCHEMA.length()];
    for (int i = 0; i < children.length; i++) {
      children[i] = vector.getChild(i);
    }

    return new CommitInfo(
        version, // Notably: do NOT read the version from the vector; infer it from the file name
        Optional.ofNullable(children[0].isNullAt(rowId) ? null : children[0].getLong(rowId)),
        children[1].isNullAt(rowId) ? null : children[1].getLong(rowId),
        children[2].isNullAt(rowId) ? null : children[2].getString(rowId),
        children[3].isNullAt(rowId) ? null : children[3].getString(rowId),
        children[4].isNullAt(rowId)
            ? Collections.emptyMap()
            : VectorUtils.toJavaMap(children[4].getMap(rowId)),
        children[5].isNullAt(rowId) ? null : children[5].getBoolean(rowId),
        children[6].isNullAt(rowId) ? null : children[6].getString(rowId),
        children[7].isNullAt(rowId)
            ? Collections.emptyMap()
            : VectorUtils.toJavaMap(children[7].getMap(rowId)));
  }

  /**
   * Returns the `inCommitTimestamp` of the given `commitInfoOpt` if it is defined. Throws an
   * exception if `commitInfoOpt` is empty or contains an empty `inCommitTimestamp`.
   */
  public static long getRequiredInCommitTimestamp(
      Optional<CommitInfo> commitInfoOpt, long version) {
    final CommitInfo commitInfo =
        commitInfoOpt.orElseThrow(() -> DeltaErrors.missingCommitInfo(Long.toString(version)));

    return commitInfo.inCommitTimestamp.orElseThrow(
        () -> DeltaErrors.missingCommitTimestamp(Long.toString(version)));
  }

  /** Get the persisted commit info (if available) for the given delta file. */
  public static Optional<CommitInfo> loadCommitInfoAtVersion(
      Engine engine, Path logPath, long version) {
    final StructType readSchema = new StructType().add("commitInfo", CommitInfo.FULL_SCHEMA);
    final FileStatus file =
        FileStatus.of(
            FileNames.deltaFile(logPath, version) /* path */,
            0 /* size */,
            0 /* modification time */);
    try (CloseableIterator<ColumnarBatch> columnarBatchIter =
        wrapEngineExceptionThrowsIO(
            () ->
                engine
                    .getJsonHandler()
                    .readJsonFiles(singletonCloseableIterator(file), readSchema, Optional.empty()),
            "Reading the commit info with schema=%s from delta file %s",
            readSchema,
            file.getPath())) {
      while (columnarBatchIter.hasNext()) {
        final ColumnarBatch columnarBatch = columnarBatchIter.next();
        assert (columnarBatch.getSchema().equals(readSchema));
        final ColumnVector commitInfoVector = columnarBatch.getColumnVector(0);
        for (int i = 0; i < commitInfoVector.getSize(); i++) {
          if (!commitInfoVector.isNullAt(i)) {
            CommitInfo commitInfo = CommitInfo.fromColumnVector(commitInfoVector, i, version);
            if (commitInfo != null) {
              return Optional.of(commitInfo);
            }
          }
        }
      }
    } catch (IOException ex) {
      throw new UncheckedIOException("Could not close iterator", ex);
    }

    logger.info("No commit info found for commit of version {}", version);
    return Optional.empty();
  }

  /////////////////////////////
  // Member Fields / Methods //
  /////////////////////////////

  /**
   * Note that the version is not written to or read from the Delta commit file; we can infer it
   * from the file name.
   */
  private final long version;

  private Optional<Long> inCommitTimestamp;
  private final long timestamp;
  private final String engineInfo;
  private final String operation;
  private final Map<String, String> operationParameters;
  private final boolean isBlindAppend;
  private final String txnId;
  private final Map<String, String> operationMetrics;

  public CommitInfo(
      long version,
      Optional<Long> inCommitTimestamp,
      long timestamp,
      String engineInfo,
      String operation,
      Map<String, String> operationParameters,
      boolean isBlindAppend,
      String txnId,
      Map<String, String> operationMetrics) {
    this.version = version;
    this.inCommitTimestamp = inCommitTimestamp;
    this.timestamp = timestamp;
    this.engineInfo = engineInfo;
    this.operation = operation;
    this.operationParameters = Collections.unmodifiableMap(operationParameters);
    this.isBlindAppend = isBlindAppend;
    this.txnId = txnId;
    this.operationMetrics = operationMetrics;
  }

  public long getVersion() {
    return version;
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
    final Map<Integer, Object> commitInfo = new HashMap<>();
    // Notably: do NOT write the version
    commitInfo.put(COL_NAME_TO_ORDINAL.get("inCommitTimestamp"), inCommitTimestamp.orElse(null));
    commitInfo.put(COL_NAME_TO_ORDINAL.get("timestamp"), timestamp);
    commitInfo.put(COL_NAME_TO_ORDINAL.get("engineInfo"), engineInfo);
    commitInfo.put(COL_NAME_TO_ORDINAL.get("operation"), operation);
    commitInfo.put(
        COL_NAME_TO_ORDINAL.get("operationParameters"), stringStringMapValue(operationParameters));
    commitInfo.put(COL_NAME_TO_ORDINAL.get("isBlindAppend"), isBlindAppend);
    commitInfo.put(COL_NAME_TO_ORDINAL.get("txnId"), txnId);
    commitInfo.put(
        COL_NAME_TO_ORDINAL.get("operationMetrics"), stringStringMapValue(operationMetrics));

    return new GenericRow(CommitInfo.FULL_SCHEMA, commitInfo);
  }

  ////////////////////////////////
  // AbstractCommitInfo methods //
  ////////////////////////////////

  @Override
  public long getCommitTimestamp() {
    return getInCommitTimestamp()
        .orElseThrow(() -> DeltaErrors.missingCommitTimestamp(Long.toString(version)));
  }
}
