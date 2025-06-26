package io.delta.dsv2.read;

import io.delta.kernel.data.FilteredColumnarBatch;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DeltaPartitionReaderOfColumnarBatch
    extends DeltaPartitionReader<org.apache.spark.sql.vectorized.ColumnarBatch> {
  private static final Logger logger =
      LoggerFactory.getLogger(DeltaPartitionReaderOfColumnarBatch.class);

  private KernelColumnarBatchToSparkColumnarBatchWrapper currentBatch = null;
  private boolean closed = false;

  public DeltaPartitionReaderOfColumnarBatch(DeltaInputPartition deltaInputPartition)
      throws IOException {
    super(deltaInputPartition);
    logger.info("DeltaPartitionReaderOfColumnarBatch constructed");
  }

  @Override
  public boolean next() {
    if (!closed) {
      if (logicalRowDataColumnarBatchIter.hasNext()) {
        FilteredColumnarBatch kernelBatch = logicalRowDataColumnarBatchIter.next();
        currentBatch = KernelColumnarBatchToSparkColumnarBatchWrapper.create(kernelBatch);
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public org.apache.spark.sql.vectorized.ColumnarBatch get() {
    if (currentBatch == null) {
      throw new NoSuchElementException("No current batch available; call next() first.");
    }
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      logicalRowDataColumnarBatchIter.close();
      if (currentBatch != null) {
        currentBatch.close();
      }
      closed = true;
    }
  }
}
