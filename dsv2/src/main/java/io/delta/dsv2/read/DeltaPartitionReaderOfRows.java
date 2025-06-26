package io.delta.dsv2.read;

import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created on the executor. */
class DeltaPartitionReaderOfRows extends DeltaPartitionReader<InternalRow> {
  private static final Logger logger = LoggerFactory.getLogger(DeltaPartitionReaderOfRows.class);

  private CloseableIterator<Row> rowIter = null;
  private Row curr = null;
  private boolean closed = false;

  public DeltaPartitionReaderOfRows(DeltaInputPartition deltaInputPartition) throws IOException {
    super(deltaInputPartition);
    logger.info("DeltaPartitionReaderOfRows constructed");
  }

  @Override
  public void close() throws IOException {
    logicalRowDataColumnarBatchIter.close();
    if (rowIter != null) {
      rowIter.close();
    }
    closed = true;
  }

  @Override
  public boolean next() {
    if (!closed) {
      // Check if there are remaining rows in the current row iterator
      if (rowIter != null && rowIter.hasNext()) {
        curr = rowIter.next();
        return true;
      }
      // If current batch is exhausted, fetch the next batch and reset the row iterator
      else if (logicalRowDataColumnarBatchIter.hasNext()) {
        rowIter = logicalRowDataColumnarBatchIter.next().getRows();
        return next(); // Recursively call next to process the new batch
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public InternalRow get() {
    if (curr == null) {
      throw new NoSuchElementException("No current row available; call next() first.");
    }
    return new KernelRowToSparkRowWrapper(curr);
  }
}
