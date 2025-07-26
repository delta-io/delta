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

  private boolean isBaseScanExhausted = false;

  private boolean hasLeastOneBatchConsumed = false;

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

  /**
   * Returns a page token representing the position of the last consumed batch, corresponding to the
   * most recent {@code next()} call.
   *
   * <p>Note: This method can be called after the paginated iterator has been closed.
   */
  @Override
  public Optional<Row> getCurrentPageToken() {
    // User must call getCurrentPageToken() after they call next() at least once.
    checkState(
        hasLeastOneBatchConsumed,
        "Can't call getCurrentPageToken() without consuming any batches!");
    // Return empty page token to signal pagination completes.
    if (isBaseScanExhausted) {
      return Optional.empty();
    }

    Row pageTokenRow =
        new PageToken(
                lastReadLogFilePath,
                lastReturnedRowIndex,
                (lastSidecarIndex == -1) ? Optional.empty() : Optional.of(lastSidecarIndex),
                Meta.KERNEL_VERSION,
                paginationContext.getTablePath(),
                paginationContext.getTableVersion(),
                paginationContext.getPredicateHash(),
                paginationContext.getLogSegmentHash())
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
    if (!baseFilteredScanFilesIter.hasNext()) {
      isBaseScanExhausted = true;
      return;
    }
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
        // All fully consumed multi-part checkpoints should already be skipped in ActionsIterator.
        // Fully consumed delta commit, log compaction and V2 checkpoint files won't be skipped in
        // ActionsIterator.
        checkArgument(!FileNames.isMultiPartCheckpointFile(batchFilePath));

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
        // If a batch is partially consumed, this means batch size is changed between page requests, and this's not allowed.
        long tokenLastIndex = tokenLastReturnedRowIndexOpt.get();
        boolean isBatchPartiallyConsumed =
            lastReturnedRowIndex < tokenLastIndex &&
                lastReturnedRowIndex + numRowsInBatch > tokenLastIndex;
        if (isBatchPartiallyConsumed) {
          throw new IllegalStateException("Pagination: batch size appears to have changed between page requests");
        }
        // Skip this batch if its last row index is smaller than or equal to last returned row index
        // in token.
        if (tokenLastReturnedRowIndexOpt.isPresent()
            && lastReturnedRowIndex + numRowsInBatch <= tokenLastReturnedRowIndexOpt.get()) {
          lastReturnedRowIndex += numRowsInBatch;
          logger.info(
              "Pagination: skipping batch from a partially consumed file : {}, "
                  + "last row index is {}",
              batchFilePath,
              lastReturnedRowIndex);
          continue;
        }
      }

      // Case 3: If this batch belongs to an "unseen file" — meaning a file whose content was
      // not read at all in any previous page. In other words, this file is fully unconsumed:
      // it was neither partially read nor fully read before.
      // Batches from such files won't be skipped.

      // currentBatch will be included in the current page.
      currentBatch = Optional.of(batch);

      // Found a valid batch, break out of the loop
      break;
    }
  }

  @Override
  public FilteredColumnarBatch next() {
    checkState(!closed, "Can't call `next` on a closed iterator.");
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    hasLeastOneBatchConsumed = true;
    final FilteredColumnarBatch ret = currentBatch.get();
    validateBatch(ret);
    final long numSelectedAddFilesInBatch = ret.getPreComputedNumSelectedRows().get();
    final String batchFilePath = ret.getFilePath().get();

    // This batch is the first one we've seen from an "unseen file" during the current page
    // read;
    // update tracking state to reflect that we're now reading an "unseen file".
    // Example:
    //   - Page 1 ends midway through 18.json.
    //   - Page 2 resumes, completes 18.json, then sees 17.json.
    //   - 17.json was not seen in Page 1, so it's an "unseen file".
    //   - In 17.json, all batches are emitted.
    //   - Only the first batch triggers state reset via `isFirstBatchFromNewFile`.
    if (isFirstBatchFromUnseenFile(batchFilePath)) {
      lastReadLogFilePath = batchFilePath;
      // Start from -1 so adding the first batch size gives correct 0-based row index.
      lastReturnedRowIndex = -1;
      logger.info("Pagination: reading new file: {}", lastReadLogFilePath);
      if (isSidecar(batchFilePath)) {
        lastSidecarIndex++;
      }
    }

    // Calculate the row index of the last row in current batch within the file.
    lastReturnedRowIndex += ret.getData().getSize();
    numScanFilesReturned += numSelectedAddFilesInBatch;

    logger.info(
        "Pagination: emit a new batch: lastReadLogFilePath: {}, lastReturnedRowIndex: {}",
        lastReadLogFilePath,
        lastReturnedRowIndex);
    logger.info("Pagination: total numScanFilesReturned: {}", numScanFilesReturned);

    currentBatch = Optional.empty();
    return ret;
  }

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
   * Returns {@code true} if the current batch comes from a fully consumed file in previous pages,
   * as determined by the page token.
   */
  private boolean isBatchFromFullyConsumedFile(
      String batchFilePath,
      Optional<String> tokenLastReadLogFilePathOpt,
      Optional<Long> tokenLastReadSidecarFileIdxOpt) {

    if (tokenLastReadSidecarFileIdxOpt.isPresent()) {
      // If a sidecar file was read in the previous page, all non-sidecar files (i.e., delta commit,
      // log compaction and V2 checkpoint) are considered fully consumed.
      return !isSidecar(batchFilePath);
    } else if (tokenLastReadLogFilePathOpt.isPresent()) {
      // Delta log files are ordered in reverse lexicographic order (i.e., higher file names appear
      // earlier).
      // If the current batch’s log file name is greater than the last one recorded in the token,
      // it means this file appeared earlier in the segment and has already been processed.
      return !isSidecar(batchFilePath)
          && batchFilePath.compareTo(tokenLastReadLogFilePathOpt.get()) > 0;
    }
    return false;
  }

  /**
   * Returns true if the current batch is from the same file (either log or sidecar) that the
   * previous page ended at, based on the file path and sidecar index recorded in the page token.
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
    if (!isSameFilePath) return false;
    // For sidecar files, if file path matches, sidecar index must also present and match.
    if (isSidecar(batchFilePath)) {
      checkArgument(
          tokenLastReadSidecarFileIdxOpt.isPresent()
              && lastSidecarIndex == tokenLastReadSidecarFileIdxOpt.get(),
          "Sidecar index mismatch for file: %s",
          batchFilePath);
    }
    return true;
  }

  /**
   * Returns {@code true} if the current batch is the first one from a different file than the last
   * seen, indicating the start of a new unseen file during pagination.
   */
  private boolean isFirstBatchFromUnseenFile(String batchFilePath) {
    // If the batch's file path differs from {@code lastReadLogFilePath}, it's considered an
    // unseen file.
    if (!batchFilePath.equals(lastReadLogFilePath)) {
      // For non-sidecar files, files must appear in reverse lexicographic order —
      // i.e., the current file must come *before* the last seen file.
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
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      Utils.closeCloseables(baseFilteredScanFilesIter);
    }
  }
}
