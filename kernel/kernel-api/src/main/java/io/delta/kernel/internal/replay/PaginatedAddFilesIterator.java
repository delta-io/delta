package io.delta.kernel.internal.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.utils.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Base64;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * We don't return partial batch. If page size is returned, we terminate pagination early.
 * We use number of batches to skip as page token.
* */
public class PaginatedAddFilesIterator implements CloseableIterator<FilteredColumnarBatch> {

    private final Iterator<FilteredColumnarBatch> originalIterator;
    private final long numAddFilesToSkip;  // how many active add files to skip
    private final long pageSize;   // max files to return in this page

    private long numAddFilesRead = 0;
    private long numAddFilesReturned = 0;
    private final String startingLogFileName;
    private String lastLogFileName; // name of the last log file read
    private long sidecarIdx;

    private FilteredColumnarBatch nextBatch = null;  // next batch ready to return

    public PaginatedAddFilesIterator(Iterator< FilteredColumnarBatch> originalIterator, PaginationContext paginationContext) {
        this.originalIterator = originalIterator;
        this.numAddFilesToSkip = paginationContext.rowIdx;
        this.startingLogFileName = paginationContext.startingLogFileName;
        this.pageSize = paginationContext.pageSize;
        this.lastLogFileName = startingLogFileName; // name of last log file we read
    //    this.isHashSetCached = paginationContext.isHashSetCached;
        this.sidecarIdx = paginationContext.sidecarIdx;
    }

    @Override
    public boolean hasNext() {
        if (nextBatch != null) {
            return true;
        }
        if (numAddFilesReturned >= pageSize) {
            return false; // page limit reached
        }
        while (originalIterator.hasNext()) {
            FilteredColumnarBatch batch = originalIterator.next(); //
            String fileName = batch.getFileName();
            if(!fileName.equals(lastLogFileName)) {
                System.out.println("reading " + fileName);
                if(fileName.endsWith(".parquet") && sidecarIdx!=-1) sidecarIdx++; // keep track of sidecarIdx
                lastLogFileName = fileName;
                numAddFilesRead = 0;
            }
            long numActiveAddFiles = batch.getNumOfTrueRows(); // lower long: primitive / Long: box primitive - this can be null; long is preferred.
            // TODO: change number of AddFiles Skipped to rowNum
            long rowNum = batch.getData().getSize(); // number of rows

            System.out.println("numActiveAddFiles: " + numActiveAddFiles);
            System.out.println("numTotalAddFiles: " + batch.getData().getColumnVector(0).getSize());

            /**
             * if sideIdx isn't -1 & current file to read is checkpoint file: sidecarIdx ++
             * return sidecarIdx;
            **/
            // if hash set is cached, first read file must be starting log file
            if(sidecarIdx==-1 && startingLogFileName.compareTo(fileName)<0) {
                // skip whole batch
                numAddFilesRead+=numActiveAddFiles;
            }
            else if (startingLogFileName.equals(fileName) && numAddFilesRead + numActiveAddFiles <= numAddFilesToSkip) {
                // skip whole batch
                numAddFilesRead+=numActiveAddFiles;
            }
            else if (numAddFilesReturned + numActiveAddFiles > pageSize) {
                System.out.println("pagination comes to an end");
                // This is the last batch to read.
                nextBatch = batch;
                numAddFilesReturned += numActiveAddFiles;
                numAddFilesRead += numActiveAddFiles;
                // terminate current pagination
                return false;
            }
            else if(startingLogFileName.equals(fileName) &&
                    numAddFilesRead + numActiveAddFiles > numAddFilesToSkip && numAddFilesRead < numAddFilesToSkip){
                // very special case: part files of current batch has been read.
                // this happens only if batch size changes between pagination request
               System.out.println("batch size changes between pagination request");
               numAddFilesRead += numActiveAddFiles;
               long numAddFilesToReturnInBatch = (numAddFilesRead + numActiveAddFiles - numAddFilesToSkip);
               numAddFilesReturned += numAddFilesToReturnInBatch;
               // TODO: unselect some rows here
               nextBatch = batch;
            }
            else {
                // no skipping needed, return full batch
                nextBatch = batch;
                numAddFilesReturned += numActiveAddFiles;
                numAddFilesRead += numActiveAddFiles;
                System.out.println("numAddFilesReturned: " + numAddFilesReturned);
                return true;
            }
            System.out.println("------------------------read one batch -------------------");
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
        // Close original iterator if it supports close (if applicable)
        if (originalIterator instanceof Closeable) {
            ((Closeable) originalIterator).close();
        }
    }

    public long getNumAddFilesToSkipForNextPage() {
        return numAddFilesRead;
    }

    public String getNextStartingLogFileName() {
        return lastLogFileName;
    }

    public PageToken getNextPageToken() {
        // return sidecarIdx-1 because first file is re-read.
        return new PageToken(lastLogFileName, numAddFilesRead, sidecarIdx-1, -1);
    }
}