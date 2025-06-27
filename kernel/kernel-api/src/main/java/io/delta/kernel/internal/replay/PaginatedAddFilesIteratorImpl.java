package io.delta.kernel.internal.replay;

import io.delta.kernel.PaginatedAddFilesIterator;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class PaginatedAddFilesIteratorImpl implements PaginatedAddFilesIterator {

  private final Iterator<FilteredColumnarBatch> originalIterator;
  private final long pageSize; // max num of files to return in this page

  private long numAddFilesReturned = 0;
  private String lastLogFileName = null; // when reading first page, lastLogFileName is absent
  private long rowIdxInLastFile = 0;
  private FilteredColumnarBatch nextBatch = null;

  public PaginatedAddFilesIteratorImpl(
      Iterator<FilteredColumnarBatch> originalIterator, PaginationContext paginationContext) {
    this.originalIterator = originalIterator;
    this.pageSize = paginationContext.pageSize;
  }

  @Override
  public boolean hasNext() {
    if (nextBatch != null) {
      return true;
    }
    if (numAddFilesReturned >= pageSize) {
      return false;
    }
    if (originalIterator.hasNext()) {
      FilteredColumnarBatch batch = originalIterator.next();
      String fileName = batch.getFileName(); // TODO: get parquet reader PR merged first
      if (!fileName.equals(lastLogFileName)) {
        lastLogFileName = fileName;
        System.out.println("fileName " + fileName);
        rowIdxInLastFile = 0; // row idx starts from 1
      }
      long numActiveAddFiles = batch.getNumOfTrueRows();
      long rowNum =
          batch.getData().getSize(); // number of rows, if 5 AddFile and 7 RemoveFile -> this is 12.

      System.out.println("numActiveAddFiles: " + numActiveAddFiles);
      System.out.println("numTotalAddFiles: " + batch.getData().getColumnVector(0).getSize());
      System.out.println("numOfRows: " + rowNum);

      nextBatch = batch;
      numAddFilesReturned += numActiveAddFiles;
      rowIdxInLastFile += rowNum;
      System.out.println("numAddFilesReturned: " + numAddFilesReturned);
      return true;
    }
    System.out.println("no batches are left");
    return false;
  }

  @Override
  public FilteredColumnarBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    FilteredColumnarBatch result = nextBatch;
    nextBatch = null;
    return result;
  }

  @Override
  public void close() throws IOException {
    if (originalIterator instanceof Closeable) {
      ((Closeable) originalIterator).close();
    }
  }

  public Row getCurrentPageToken() {
    return new PageToken(lastLogFileName, rowIdxInLastFile).getRow();
  }
}
