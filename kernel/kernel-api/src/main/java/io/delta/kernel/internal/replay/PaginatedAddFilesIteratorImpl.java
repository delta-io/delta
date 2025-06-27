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
  private String currentLogFileName = null; // when reading first page, lastLogFileName is absent
  private long currentRowIdxInLastFile = 0;
  private FilteredColumnarBatch nextBatch = null;
  private String startingLogFileName;
  private long startingRowIdxInLastFile;

  public PaginatedAddFilesIteratorImpl(
      Iterator<FilteredColumnarBatch> originalIterator, PaginationContext paginationContext) {
    this.originalIterator = originalIterator;
    this.pageSize = paginationContext.pageSize;
    this.startingLogFileName = paginationContext.lastReadLogFileName;
    this.startingRowIdxInLastFile = paginationContext.lastReadRowIdxInFile;
  }

  @Override
  public boolean hasNext() {
    if (nextBatch != null) {
      return true;
    }
    if (numAddFilesReturned >= pageSize) {
      return false;
    }
    while (originalIterator.hasNext()) {
      nextBatch = originalIterator.next();
      String fileName = nextBatch.getFileName(); // TODO: get parquet reader PR merged first
      if (!fileName.equals(currentLogFileName)) {
        currentLogFileName = fileName;
        System.out.println("fileName " + fileName);
        currentRowIdxInLastFile = 0;// row idx starts from 1
      }
      long numActiveAddFiles = nextBatch.getNumOfTrueRows();
      long rowNum = nextBatch.getData().getSize();
      currentRowIdxInLastFile += rowNum;

      System.out.println("numActiveAddFiles: " + numActiveAddFiles);
      System.out.println("numTotalAddFiles: " + nextBatch.getData().getColumnVector(0).getSize());
      System.out.println("numOfRows: " + rowNum);

      if(startingLogFileName!=null && currentLogFileName.compareTo(startingLogFileName) > 0 ||
          (currentLogFileName.equals(startingLogFileName) && currentRowIdxInLastFile < startingRowIdxInLastFile)) {
        continue;
      }
      numAddFilesReturned += numActiveAddFiles;
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
    return new PageToken(currentLogFileName, currentRowIdxInLastFile).getRow();
  }
}
