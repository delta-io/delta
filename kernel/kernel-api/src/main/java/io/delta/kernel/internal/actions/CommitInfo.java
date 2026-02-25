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
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
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
public class CommitInfo {

  //////////////////////////////////
  // Static variables and methods //
  //////////////////////////////////

  public static final StructType FULL_SCHEMA =
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

  private static final StructType READ_SCHEMA =
      new StructType().add("commitInfo", CommitInfo.FULL_SCHEMA);

  private static final Map<String, Integer> COL_NAME_TO_ORDINAL =
      IntStream.range(0, FULL_SCHEMA.length())
          .boxed()
          .collect(toMap(i -> FULL_SCHEMA.at(i).getName(), i -> i));

  private static final Logger logger = LoggerFactory.getLogger(CommitInfo.class);

  public static CommitInfo fromColumnVector(ColumnVector vector, int rowId) {
    if (vector.isNullAt(rowId)) {
      return null;
    }

    ColumnVector[] children = new ColumnVector[8];
    for (int i = 0; i < children.length; i++) {
      children[i] = vector.getChild(i);
    }

    return new CommitInfo(
        Optional.ofNullable(children[0].isNullAt(rowId) ? null : children[0].getLong(rowId)),
        children[1].isNullAt(rowId) ? null : children[1].getLong(rowId),
        children[2].isNullAt(rowId) ? null : children[2].getString(rowId),
        children[3].isNullAt(rowId) ? null : children[3].getString(rowId),
        children[4].isNullAt(rowId)
            ? Collections.emptyMap()
            : VectorUtils.toJavaMap(children[4].getMap(rowId)),
        Optional.ofNullable(children[5].isNullAt(rowId) ? null : children[5].getBoolean(rowId)),
        children[6].isNullAt(rowId) ? null : children[6].getString(rowId),
        children[7].isNullAt(rowId)
            ? Collections.emptyMap()
            : VectorUtils.toJavaMap(children[7].getMap(rowId)));
  }

  /**
   * Returns the `inCommitTimestamp` of delta file at the requested version. Throws an exception if
   * the delta file does not exist or does not have a commitInfo action or if the commitInfo action
   * contains an empty `inCommitTimestamp`.
   *
   * <p><strong>WARNING: UNSAFE METHOD</strong> because this assumes that 00N.json is published.
   */
  // TODO: [delta-io/delta#5147] Can't just use the logPath & version on catalogManaged tables.
  public static long unsafeGetRequiredIctFromPublishedDeltaFile(
      Engine engine, Path logPath, long version) {
    return extractRequiredIctFromCommitInfoOpt(
        unsafeTryReadCommitInfoFromPublishedDeltaFile(engine, logPath, version), version, logPath);
  }

  /**
   * Returns the `inCommitTimestamp` of the provided delta file. Throws an exception if the delta
   * file does not exist or does not have a commitInfo action or if the commitInfo action contains
   * an empty `inCommitTimestamp`. The delta file can be either a published or staged commit file.
   */
  public static long getRequiredIctFromDeltaFile(
      Engine engine, Path tablePath, FileStatus deltaFileStatus, long version) {
    checkArgument(
        FileNames.isCommitFile(deltaFileStatus.getPath()), "Must provide a valid commit file");
    return extractRequiredIctFromCommitInfoOpt(
        tryReadCommitInfoFromDeltaFile(engine, deltaFileStatus), version, tablePath);
  }

  /**
   * Returns the `inCommitTimestamp` of the given `commitInfoOpt` if it is defined. Throws an
   * exception if `commitInfoOpt` is empty or contains an empty `inCommitTimestamp`.
   */
  // TODO: [delta-io/delta#5147] Can't just use the logPath & version on catalogManaged tables.
  public static long extractRequiredIctFromCommitInfoOpt(
      Optional<CommitInfo> commitInfoOpt, long version, Path dataPath) {
    CommitInfo commitInfo =
        commitInfoOpt.orElseThrow(
            () -> DeltaErrors.tableWithIctMissingCommitInfo(dataPath.toString(), version));
    return commitInfo.inCommitTimestamp.orElseThrow(
        () -> DeltaErrors.tableWithIctMissingIct(dataPath.toString(), version));
  }

  /**
   * Get the CommitInfo action (if available) from the delta file at the given logPath and version.
   *
   * <p><strong>WARNING: UNSAFE METHOD</strong> because this assumes that 00N.json is published.
   */
  // TODO: [delta-io/delta#5147] Can't just use the logPath & version on catalogManaged tables.
  public static Optional<CommitInfo> unsafeTryReadCommitInfoFromPublishedDeltaFile(
      Engine engine, Path logPath, long version) {
    final FileStatus file =
        FileStatus.of(
            FileNames.deltaFile(logPath, version), /* path */
            0, /* size */
            0 /* modification time */);

    return tryReadCommitInfoFromDeltaFile(engine, file);
  }

  /** Read the CommitInfo action (if available) from the given delta file. */
  public static Optional<CommitInfo> tryReadCommitInfoFromDeltaFile(
      Engine engine, FileStatus deltaFileStatus) {
    try (CloseableIterator<ColumnarBatch> columnarBatchIter =
        wrapEngineExceptionThrowsIO(
            () ->
                engine
                    .getJsonHandler()
                    .readJsonFiles(
                        singletonCloseableIterator(deltaFileStatus), READ_SCHEMA, Optional.empty()),
            "Reading the CommitInfo with schema=%s from delta file %s",
            READ_SCHEMA,
            deltaFileStatus.getPath())) {
      while (columnarBatchIter.hasNext()) {
        final ColumnarBatch columnarBatch = columnarBatchIter.next();
        assert (columnarBatch.getSchema().equals(READ_SCHEMA));
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
      throw new UncheckedIOException("Could not close iterator", ex);
    }

    logger.info("No CommitInfo found in delta file {}", deltaFileStatus.getPath());
    return Optional.empty();
  }

  //////////////////////////////////
  // Member variables and methods //
  //////////////////////////////////

  private final long timestamp;
  private final String engineInfo;
  private final String operation;
  private final Map<String, String> operationParameters;
  private final Optional<Boolean> isBlindAppend;
  private final String txnId;
  private Optional<Long> inCommitTimestamp;
  private final Map<String, String> operationMetrics;

  public CommitInfo(
      Optional<Long> inCommitTimestamp,
      long timestamp,
      String engineInfo,
      String operation,
      Map<String, String> operationParameters,
      Optional<Boolean> isBlindAppend,
      String txnId,
      Map<String, String> operationMetrics) {
    this.inCommitTimestamp = requireNonNull(inCommitTimestamp);
    this.timestamp = timestamp;
    this.engineInfo = requireNonNull(engineInfo);
    this.operation = requireNonNull(operation);
    this.operationParameters = Collections.unmodifiableMap(requireNonNull(operationParameters));
    this.isBlindAppend = requireNonNull(isBlindAppend);
    this.txnId = requireNonNull(txnId);
    this.operationMetrics = Collections.unmodifiableMap(requireNonNull(operationMetrics));
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

  public Map<String, String> getOperationParameters() {
    return operationParameters;
  }

  public Optional<Boolean> getIsBlindAppend() {
    return isBlindAppend;
  }

  public String getTxnId() {
    return txnId;
  }

  public Optional<Long> getInCommitTimestamp() {
    return inCommitTimestamp;
  }

  public Map<String, String> getOperationMetrics() {
    return operationMetrics;
  }

  public void setInCommitTimestamp(Optional<Long> inCommitTimestamp) {
    this.inCommitTimestamp = inCommitTimestamp;
  }

  /**
   * Encode as a {@link Row} object with the schema {@link CommitInfo#FULL_SCHEMA}.
   *
   * @return {@link Row} object with the schema {@link CommitInfo#FULL_SCHEMA}
   */
  public Row toRow() {
    Map<Integer, Object> commitInfo = new HashMap<>();
    commitInfo.put(COL_NAME_TO_ORDINAL.get("inCommitTimestamp"), inCommitTimestamp.orElse(null));
    commitInfo.put(COL_NAME_TO_ORDINAL.get("timestamp"), timestamp);
    commitInfo.put(COL_NAME_TO_ORDINAL.get("engineInfo"), engineInfo);
    commitInfo.put(COL_NAME_TO_ORDINAL.get("operation"), operation);
    commitInfo.put(
        COL_NAME_TO_ORDINAL.get("operationParameters"), stringStringMapValue(operationParameters));
    commitInfo.put(COL_NAME_TO_ORDINAL.get("isBlindAppend"), isBlindAppend.orElse(null));
    commitInfo.put(COL_NAME_TO_ORDINAL.get("txnId"), txnId);
    commitInfo.put(
        COL_NAME_TO_ORDINAL.get("operationMetrics"), stringStringMapValue(operationMetrics));

    return new GenericRow(CommitInfo.FULL_SCHEMA, commitInfo);
  }
}
