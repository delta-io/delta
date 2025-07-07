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

import io.delta.kernel.PaginatedScanFilesIterator;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;

/** Implementation of {@link PaginatedScanFilesIterator} */
public class PaginatedScanFilesIteratorImpl implements PaginatedScanFilesIterator {

  private final CloseableIterator<FilteredColumnarBatch> originalIterator;
  private final long pageSize; // max num of files to return in this page

  public PaginatedScanFilesIteratorImpl(
      CloseableIterator<FilteredColumnarBatch> originalIterator,
      PaginationContext paginationContext) {
    this.originalIterator = originalIterator;
    this.pageSize = paginationContext.getPageSize();
  }

  @Override
  public Row getCurrentPageToken() {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public FilteredColumnarBatch next() {
    return null;
  }

  @Override
  public void close() throws IOException {}
}
