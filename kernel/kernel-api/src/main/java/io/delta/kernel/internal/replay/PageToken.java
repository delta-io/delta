/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.replay;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Page Token Class for Pagination Support */
public class PageToken {

  public static PageToken fromRow(Row row) {
    requireNonNull(row);

    // Check #1: Correct schema
    checkArgument(
        PAGE_TOKEN_SCHEMA.equals(row.getSchema()),
        String.format(
            "Invalid Page Token: input row schema does not match expected PageToken schema.\nExpected: %s\nGot: %s",
            PAGE_TOKEN_SCHEMA, row.getSchema()));

    // Check #2: All required fields are present
    for (int i = 0; i < PAGE_TOKEN_SCHEMA.length(); i++) {
      if (PAGE_TOKEN_SCHEMA.at(i).getName().equals("lastReadSidecarFileIdx")) continue;
      checkArgument(
          !row.isNullAt(i),
          String.format(
              "Invalid Page Token: required field '%s' is null at index %d",
              PAGE_TOKEN_SCHEMA.at(i).getName(), i));
    }

    return new PageToken(
        row.getString(0), // lastReadLogFileName
        row.getLong(1), // lastReturnedRowIndex
        Optional.ofNullable(row.isNullAt(2) ? null : row.getLong(2)), // lastReadSidecarFileIdx
        row.getString(3), // kernelVersion
        row.getString(4), // tablePath
        row.getLong(5), // tableVersion
        row.getLong(6), // predicateHash
        row.getLong(7)); // logSegmentHash
  }

  public static final StructType PAGE_TOKEN_SCHEMA =
      new StructType()
          .add("lastReadLogFileName", StringType.STRING, false /* nullable */)
          .add("lastReturnedRowIndex", LongType.LONG, false /* nullable */)
          .add("lastReadSidecarFileIdx", LongType.LONG, true /* nullable */)
          .add("kernelVersion", StringType.STRING, false /* nullable */)
          .add("tablePath", StringType.STRING, false /* nullable */)
          .add("tableVersion", LongType.LONG, false /* nullable */)
          .add("predicateHash", LongType.LONG, false /* nullable */)
          .add("logSegmentHash", LongType.LONG, false /* nullable */);

  // ===== Variables to mark where the last page ended (and the current page starts) =====

  /** The last log file read in the previous page. */
  private final String lastReadLogFileName;

  /**
   * The index of the last row that was returned from the last read log file during the previous
   * page. This row index is relative to the file. The current page should begin from the row
   * immediately after this row index.
   */
  private final long lastReturnedRowIndex;

  /**
   * Optional index of the last sidecar checkpoint file read in the previous page. This index is
   * based on the ordering of sidecar files in the V2 manifest checkpoint file. If present, it must
   * represent the final sidecar file that was read and must correspond to the same file as
   * `lastReadLogFileName`.
   */
  private final Optional<Long> lastReadSidecarFileIdx;

  // ===== Variables for validating query params and detecting changes in log segment =====
  private final String kernelVersion;
  private final String tablePath;
  private final long tableVersion;
  private final long predicateHash;
  private final long logSegmentHash;

  public PageToken(
      String lastReadLogFileName,
      long lastReturnedRowIndex,
      Optional<Long> lastReadSidecarFileIdx,
      String kernelVersion,
      String tablePath,
      long tableVersion,
      long predicateHash,
      long logSegmentHash) {
    this.lastReadLogFileName = requireNonNull(lastReadLogFileName, "lastReadLogFileName is null");
    this.lastReturnedRowIndex = lastReturnedRowIndex;
    this.lastReadSidecarFileIdx = lastReadSidecarFileIdx;
    this.kernelVersion = requireNonNull(kernelVersion, "kernelVersion is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.tableVersion = tableVersion;
    this.predicateHash = predicateHash;
    this.logSegmentHash = logSegmentHash;
  }

  public Row toRow() {
    Map<Integer, Object> pageTokenMap = new HashMap<>();
    pageTokenMap.put(0, lastReadLogFileName);
    pageTokenMap.put(1, lastReturnedRowIndex);
    pageTokenMap.put(2, lastReadSidecarFileIdx.orElse(null));
    pageTokenMap.put(3, kernelVersion);
    pageTokenMap.put(4, tablePath);
    pageTokenMap.put(5, tableVersion);
    pageTokenMap.put(6, predicateHash);
    pageTokenMap.put(7, logSegmentHash);

    return new GenericRow(PAGE_TOKEN_SCHEMA, pageTokenMap);
  }

  public String getLastReadLogFileName() {
    return lastReadLogFileName;
  }

  public long getLastReturnedRowIndex() {
    return lastReturnedRowIndex;
  }

  public Optional<Long> getLastReadSidecarFileIdx() {
    return lastReadSidecarFileIdx;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    PageToken other = (PageToken) obj;

    return lastReturnedRowIndex == other.lastReturnedRowIndex
        && tableVersion == other.tableVersion
        && predicateHash == other.predicateHash
        && logSegmentHash == other.logSegmentHash
        && Objects.equals(lastReadSidecarFileIdx, other.lastReadSidecarFileIdx)
        && Objects.equals(lastReadLogFileName, other.lastReadLogFileName)
        && Objects.equals(kernelVersion, other.kernelVersion)
        && Objects.equals(tablePath, other.tablePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        lastReadLogFileName,
        lastReturnedRowIndex,
        lastReadSidecarFileIdx,
        kernelVersion,
        tablePath,
        tableVersion,
        predicateHash,
        logSegmentHash);
  }
}
