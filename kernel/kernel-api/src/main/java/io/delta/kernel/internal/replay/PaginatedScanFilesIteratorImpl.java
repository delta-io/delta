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

      // Case 1: if the batch's from a fully consumed file (all data have been included in previous
      // pages), skip it. For example, if last page ends at 13.json, all batches from 14.json and
      // 15.json will be skipped
      if (isBatchFromFullyConsumedFile(
          batchFilePath, tokenLastReadFilePathOpt, tokenLastReadSidecarFileIdxOpt)) {
        // Only fully consumed JSON files and V2 manifest files won't be skipped in ActionsIterator.
        checkArgument(
            batchFilePath.endsWith(".json") || FileNames.isV2CheckpointFile(batchFilePath));
        continue;
      }

      // Case 2: if the batch is from the same last read file as recorded in the page token, decide
      // if to skip it based on row index.
      // For example, if last page ends at 13.json, we will skip batches in 13.json based on its row
      // index.
      else if (isBatchFromLastFileInToken(
          batchFilePath, tokenLastReadFilePathOpt, tokenLastReadSidecarFileIdxOpt)) {
        // Compare batch's row index to page token row
        Optional<Long> tokenRowIndex = paginationContext.getLastReturnedRowIndex();
        checkArgument(tokenRowIndex.isPresent(), "Token row index is empty!");
        // calculate the row index of the last row in current batch within the file
        lastReturnedRowIndex += numRowsInBatch;
        // Skip this batch if its last row index is smaller than or equal to the value in token.
        if (lastReturnedRowIndex <= tokenRowIndex.get()) {
          continue;
        }
      }

      // Case 3: if the batch is from a new file that has never been read in previous pages, don't
      // skip it.
      // For example, if last page ends at 13.json, all batches in 12.json will be raed (until page
      // size is reached).
      else {
        // Re-assign lastReadLogFilePath and lastSidecarIndex if needed.
        if (isBatchFromNewFile(batchFilePath)) {
          lastReadLogFilePath = batchFilePath;
          // Reset row index if this batch is from a new file not yet seen in previous pages.
          // Start from -1 so adding the first batch size gives correct 0-based row index.
          lastReturnedRowIndex = -1;
          logger.info("Reading new file: {}", lastReadLogFilePath);

          // Sidecar index starts at -1 if none was seen in the previous page.
          if (isSidecar(batchFilePath)) {
            lastSidecarIndex++;
          }
        }
        // calculate the row index of the last row in current batch within the file
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
   * Returns true if the current batch is from a fully consumed file based on the page token.
   *
   * <p>Skips conditions: - If a sidecar file was read in the previous page, we skip all log files
   * in the current page. - If the batch is a log file that appears earlier (in reverse
   * lexicographic order) than the file recorded in the token, it has already been fully processed
   * and should be skipped.
   */
  private boolean isBatchFromFullyConsumedFile(
      String batchFilePath,
      Optional<String> tokenFilePathOpt,
      Optional<Long> tokenSidecarIndexOpt) {

    if (tokenSidecarIndexOpt.isPresent() && !isSidecar(batchFilePath)) {
      return true;
    }

    return !isSidecar(batchFilePath)
        && tokenFilePathOpt.isPresent()
        && batchFilePath.compareTo(tokenFilePathOpt.get()) > 0;
  }

  /**
   * Check if this batch is from the same file (log or sidecar) that the previous page ended at, as
   * indicated by the file path or sidecar index in the page token.
   */
  private boolean isBatchFromLastFileInToken(
      String batchFilePath,
      Optional<String> tokenFilePathOpt,
      Optional<Long> tokenSidecarIndexOpt) {
    // Match if it's the same log file as recorded in the page token.
    boolean isSameLogFile =
        !isSidecar(batchFilePath)
            && tokenFilePathOpt.isPresent()
            && batchFilePath.equals(tokenFilePathOpt.get());

    // Match if it's the same sidecar file (by index) as recorded in the page token.
    boolean isSameSidecarFile =
        isSidecar(batchFilePath)
            && tokenSidecarIndexOpt.isPresent()
            && lastSidecarIndex == tokenSidecarIndexOpt.get();

    return isSameLogFile || isSameSidecarFile;
  }

  /** Check if this batch is from a new file (log or sidecar) that have never been read. */
  private boolean isBatchFromNewFile(String batchFilePath) {
    if (!batchFilePath.equals(lastReadLogFilePath)) {
      // If batch isn't from a sidecar, it must come before lastReadLogFilePath.
      checkArgument(
          isSidecar(batchFilePath)
              || lastReadLogFilePath == null
              || batchFilePath.compareTo(lastReadLogFilePath) < 0);
      return true;
    }
    return false;
  }

  private boolean isSidecar(String filePath) {
    if (filePath.contains("/_delta_log/_sidecars/") && filePath.endsWith(".parquet")) {
      throw new UnsupportedOperationException("Sidecar file isn't supported yet!");
    }
    return false;
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
