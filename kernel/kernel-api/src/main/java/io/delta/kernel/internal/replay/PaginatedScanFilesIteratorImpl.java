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
import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.Meta;
import io.delta.kernel.PaginatedScanFilesIterator;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of {@link PaginatedScanFilesIterator} */
public class PaginatedScanFilesIteratorImpl implements PaginatedScanFilesIterator {

  private static final Logger logger =
      LoggerFactory.getLogger(PaginatedScanFilesIteratorImpl.class);

  /**
   * Filtered ScanFiles iterator from the base scan, excluding batches from fully consumed log
   * files. For example, if previous pages have fully consumed sidecar files A and B, and partially
   * consumed sidecar C, this iterator will exclude all batches from A and B, but include all
   * batches from C.
   *
   * <p>Note: When no cached hash sets are available, this iterator will include all batches from
   * JSON log files.
   */
  private final CloseableIterator<FilteredColumnarBatch> baseFilteredScanFilesIter;

  /** Pagination Context that carries page token and page size info. */
  private final PaginationContext paginationContext;

  /** Maximum number of ScanFiles to include in current page */
  private final long pageSize;

  /** Total number of ScanFiles returned in current page */
  private long numScanFilesReturned;

  /**
   * The name of the last log file that was read during pagination.
   *
   * <p>This value is used to track which log file the current scan is processing.
   *
   * <p>Initialization:
   *
   * <ul>
   *   <li>If the pagination token includes a log file path, this value is initialized from it.
   *   <li>If the pagination token does not include a log file path (i.e., the previous page did not
   *       read any log file), this value is initialized to {@code null}.
   * </ul>
   */
  private String lastReadLogFilePath = null;

  /**
   * Tracks the index of the last read sidecar file during pagination.
   *
   * <p>The index starts from 0 for the first sidecar file read. It is incremented each time a new
   * sidecar file is encountered during scanning.
   *
   * <p>Initialization:
   *
   * <ul>
   *   <li>If the pagination token includes a sidecar index, this value is initialized from it.
   *   <li>If the pagination token does not include a sidecar index (i.e., no sidecar file was read
   *       in the previous page), this value is initialized to {@code -1}.
   * </ul>
   */
  private long lastSidecarIndex = -1;

  /**
   * The index of the last row returned from the last log file that was read.
   *
   * <p>For example, if the last page contains 3 batches from the same log file, and each batch has
   * 10 rows, this value will be 29 (since row indices start at 0).
   *
   * <p>This value corresponds to the one saved in the page token if present.
   */
  private long lastReturnedRowIndex = -1;

  private Optional<FilteredColumnarBatch> currentBatch = Optional.empty();

  private boolean closed = false;

  /**
   * Constructs a paginated iterator over scan files on top of a given filtered scan files iterator
   * and pagination context.
   *
   * @param baseFilteredScanFilesIter The underlying scan files iterator with data skipping and
   *     partition pruning applied. This iterator serves as the source of filtered scan results for
   *     pagination.
   * @param paginationContext The pagination context that carries pagination-related information,
   *     such as the maximum number of files to return in a page.
   */
  public PaginatedScanFilesIteratorImpl(
      CloseableIterator<FilteredColumnarBatch> baseFilteredScanFilesIter,
      PaginationContext paginationContext) {
    this.baseFilteredScanFilesIter = baseFilteredScanFilesIter;
    this.paginationContext = paginationContext;
    this.pageSize = paginationContext.getPageSize();
    if (paginationContext.getLastReadLogFilePath().isPresent()) {
      lastReadLogFilePath = paginationContext.getLastReadLogFilePath().get();
    }
    if (paginationContext.getLastReadSidecarFileIdx().isPresent()) {
      lastSidecarIndex = paginationContext.getLastReadSidecarFileIdx().get();
    }
  }

  @Override
  public Optional<Row> getCurrentPageToken() {
    if (!baseFilteredScanFilesIter.hasNext()) {
      return Optional.empty();
    }
    // TODO: replace hash value of predicate and log segment
    Row pageTokenRow =
        new PageToken(
                lastReadLogFilePath,
                lastReturnedRowIndex,
                (lastSidecarIndex == -1)
                    ? Optional.empty()
                    : Optional.of(lastSidecarIndex) /* sidecar file index */,
                Meta.KERNEL_VERSION,
                paginationContext.getTablePath() /* table path */,
                paginationContext.getTableVersion() /* table version */,
                -1 /* predicate hash */,
                -1 /* log segment hash */)
            .toRow();
    return Optional.of(pageTokenRow);
  }

  @Override
  public boolean hasNext() {
    checkState(!closed, "Can't call `hasNext` on a closed iterator.");
    if (!currentBatch.isPresent()) {
      prepareNext();
    }
    return currentBatch.isPresent();
  }

  /**
   * Prepares the next FilteredColumnarBatch to return in the current page. Skips batches that have
   * already been returned in the previous page, based on file path, row index and sidecar index
   * stored in the pagination context.
   */
  private void prepareNext() {
    if (currentBatch.isPresent()) return;
    if (!baseFilteredScanFilesIter.hasNext()) return;
    if (numScanFilesReturned >= pageSize) return;

    Optional<String> tokenLastReadFilePathOpt = paginationContext.getLastReadLogFilePath();
    Optional<Long> tokenLastReadSidecarFileIdxOpt = paginationContext.getLastReadSidecarFileIdx();

    while (baseFilteredScanFilesIter.hasNext() && numScanFilesReturned < pageSize) {
      final FilteredColumnarBatch batch = baseFilteredScanFilesIter.next();

      validateBatch(batch);

      final String batchFilePath = batch.getFilePath().get();
      final long numRowsInBatch = batch.getData().getSize();

      // Case 1: Skip batches from fully consumed files.
      // A file is considered fully consumed if it appears earlier (in reverse lexicographic order)
      // than the last read file recorded in the page token.
      //
      // Example:
      //   - Suppose the previous page ends at file 13.json
      //   - Then, all files after 13.json (e.g., 14.json, 15.json, etc.)
      //     have already been processed and are considered fully consumed.
      //   - Any batches from these files should be skipped here.
      if (isBatchFromFullyConsumedFile(
          batchFilePath, tokenLastReadFilePathOpt, tokenLastReadSidecarFileIdxOpt)) {
        // Only fully consumed JSON files and V2 manifest files won't be skipped in ActionsIterator.
        checkArgument(
            batchFilePath.endsWith(".json") || FileNames.isV2CheckpointFile(batchFilePath));
        logger.info("Pagination: skipping batch from a fully consumed file : {}", batchFilePath);
        continue;
      }

      // Case 2: The batch belongs to the same last read log file recorded in the page token.
      // In this case, we may have partially consumed this file on the previous page,
      // so we need to decide whether to skip the current batch based on row index.
      //
      // Example:
      //   - Page 1 ends after processing row index 9 in file 13.json (i.e., the first 10 rows).
      //   - This includes two batches: batch 1 (rows 0–4), batch 2 (rows 5–9).
      //   - When reading page 2, we may re-encounter these batches.
      //     * Batch 1 ends at row 4 → skip (already returned).
      //     * Batch 2 ends at row 9 → skip (already returned).
      //     * Batch 3 starts at row 10 → keep (new data).
      else if (isBatchFromLastFileInToken(
          batchFilePath, tokenLastReadFilePathOpt, tokenLastReadSidecarFileIdxOpt)) {
        Optional<Long> tokenLastReturnedRowIndexOpt = paginationContext.getLastReturnedRowIndex();
        // Calculate the row index of the last row in current batch within the file.
        lastReturnedRowIndex += numRowsInBatch;
        // Skip this batch if its last row index is smaller than or equal to last returned row index
        // in token.
        if (tokenLastReturnedRowIndexOpt.isPresent()
            && lastReturnedRowIndex <= tokenLastReturnedRowIndexOpt.get()) {
          logger.info(
              "Pagination: skipping batch from a partially consumed file : {}", batchFilePath);
          continue;
        }
      }

      // Case 3: The batch is from a completely new file - one that has not been seen in previous
      // pages.
      // Do not skip it — this file is still fully unread.
      // Example:
      //   - Suppose the last page ended at file 13.json.
      //   - In reverse lexicographic order, the next files to process will be 12.json, 11.json,
      // etc.
      //   - These files are considered new (unseen) and all their batches should be processed
      //     until the page size is reached.
      else {
        // This batch is the first one we've seen from a new file during the current page read;
        // update tracking state to reflect that we're now reading this file.
        if (isFirstBatchFromNewFile(batchFilePath)) {
          lastReadLogFilePath = batchFilePath;
          // Start from -1 so adding the first batch size gives correct 0-based row index.
          lastReturnedRowIndex = -1;
          logger.info("Pagination: reading new file: {}", lastReadLogFilePath);
          if (isSidecar(batchFilePath)) {
            lastSidecarIndex++;
          }
        }
        // Calculate the row index of the last row in current batch within the file.
        lastReturnedRowIndex += numRowsInBatch;
      }

      // currentBatch will be included in the current page.
      currentBatch = Optional.of(batch);
      final long numSelectedAddFilesInBatch = batch.getPreComputedNumSelectedRows().get();
      numScanFilesReturned += numSelectedAddFilesInBatch;

      logger.info("total numScanFilesReturned: {}", numScanFilesReturned);
      logger.info(
          "numSelectedAddFilesInBatch: {}, numRowsInBatch: {}",
          numSelectedAddFilesInBatch,
          numRowsInBatch);

      // Found a valid batch, break out of the loop
      break;
    }
  }

  /** Validate current batch. */
  void validateBatch(FilteredColumnarBatch batch) {
    // FilePath and pre-computed number of selected rows are expected to be present; both are
    // computed and set in ActiveAddFilesIterator (when building FilteredColumnarBatch from
    // ActionWrapper)
    checkArgument(batch.getFilePath().isPresent(), "File path doesn't exist!");
    checkArgument(
        batch.getPreComputedNumSelectedRows().isPresent(),
        "Pre-computed number of selected rows doesn't exist!");
  }

  /**
   * Returns {@code true} if the current batch comes from a fully consumed file, as determined by
   * the page token.
   *
   * <p>Logic:
   *
   * <ul>
   *   <li>If a sidecar file was read in the previous page, all non-sidecar files (i.e., JSON and V2 manifest)
   *  *       are considered consumed.
   *   <li>Delta log files are ordered in reverse lexicographic order (i.e., higher file names appear earlier).
   *  *       If the current batch’s log file name is greater than the last one recorded in the token,
   *  *       it means this file appeared earlier in the segment and has already been processed.
   * </ul>
   */
  private boolean isBatchFromFullyConsumedFile(
      String batchFilePath,
      Optional<String> tokenLastReadLogFilePathOpt,
      Optional<Long> tokenLastReadSidecarFileIdxOpt) {
    if (tokenLastReadSidecarFileIdxOpt.isPresent()) {
      // All non-sidecar files are considered fully consumed if a sidecar file was read in last page.
      return !isSidecar(batchFilePath);
    }
    else if (tokenLastReadLogFilePathOpt.isPresent()) {
      // In reverse lexicographic order, files with "larger" names are consumed earlier, so we skip those.
      return !isSidecar(batchFilePath) && batchFilePath.compareTo(tokenLastReadLogFilePathOpt.get()) > 0;
    }
    return false;
  }

  /**
   * Returns true if the current batch is from the same file (either log or sidecar) that the
   * previous page ended at, based on the file path and sidecar index recorded in the page token.
   *
   * <p>Logic:
   *
   * <ul>
   *   <li>For regular log files: returns true if the file path matches the token’s last read path.
   *   <li>For sidecar files: additionally checks the sidecar index presents and matches.
   * </ul>
   *
   * @param batchFilePath Path of the current batch file.
   * @param tokenLastReadFilePathOpt Last fully or partially read file recorded in the pagination
   *     token.
   * @param tokenLastReadSidecarFileIdxOpt Sidecar index from the token, if applicable.
   * @return true if the batch comes from the same file as the one where the previous page ended.
   */
  private boolean isBatchFromLastFileInToken(
      String batchFilePath,
      Optional<String> tokenLastReadFilePathOpt,
      Optional<Long> tokenLastReadSidecarFileIdxOpt) {
    // Check if batch file path matches last read file path recorded in the page token (if
    // present).
    boolean isSameFilePath =
        tokenLastReadFilePathOpt.isPresent()
            && batchFilePath.equals(tokenLastReadFilePathOpt.get());
    if (isSameFilePath) {
      // If file path matches, sidecar index must also present and match.
      if (isSidecar(batchFilePath)) {
        checkArgument(
            tokenLastReadSidecarFileIdxOpt.isPresent()
                && lastSidecarIndex == tokenLastReadSidecarFileIdxOpt.get(),
            "Sidecar index mismatch for file: %s", batchFilePath);
      }
      return true;
    }
    return false;
  }

  /**
   * Returns {@code true} if the current batch is the first one from a different file than the
   * last seen, indicating the start of a new file during pagination.
   *
   * <p>Logic:
   *
   * <ul>
   *   <li>If the batch's file path differs from {@code lastReadLogFilePath}, it's considered a new file.
   *   <li>For non-sidecar files, files must appear in reverse lexicographic order —
   *       i.e., the current file must come *before* the last seen file.
   * </ul>
   *
   * @param batchFilePath The path of the current batch's file.
   * @return {@code true} if this is the first batch from a new file not yet seen in the current page; {@code false} otherwise.
   * @throws IllegalArgumentException if a non-sidecar file appears out of expected order.
   */
  private boolean isFirstBatchFromNewFile(String batchFilePath) {
    if (!batchFilePath.equals(lastReadLogFilePath)) {
      // If batch isn't from a sidecar, it must appear after than lastReadLogFilePath in reverse lexicographic order.
      checkArgument(
          isSidecar(batchFilePath)
              || lastReadLogFilePath == null
              || batchFilePath.compareTo(lastReadLogFilePath) < 0,
          "Expected file '%s' to appear after last read file '%s' in reverse lexicographic order, "
              + "unless it's a sidecar file",
          batchFilePath,
          lastReadLogFilePath);
      return true;
    }
    return false;
  }

  // TODO: move isSidecar() to FileNames
  private boolean isSidecar(String filePath) {
    return filePath.contains("/_delta_log/_sidecars/") && filePath.endsWith(".parquet");
  }

  @Override
  public FilteredColumnarBatch next() {
    checkState(!closed, "Can't call `next` on a closed iterator.");
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final FilteredColumnarBatch ret = currentBatch.get();
    currentBatch = Optional.empty();
    return ret;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      Utils.closeCloseables(baseFilteredScanFilesIter);
    }
  }
}
