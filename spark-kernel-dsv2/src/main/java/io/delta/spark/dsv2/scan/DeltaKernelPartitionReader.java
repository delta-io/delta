package io.delta.spark.dsv2.scan;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.spark.dsv2.scan.internal.KernelRowToSparkRowWrapper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;

/**
 * A PartitionReader implementation for Delta Kernel scans. This reads data from Delta tables using
 * Delta Kernel.
 */
public class DeltaKernelPartitionReader implements PartitionReader<InternalRow> {

  private final DefaultEngine engine;
  private final Row scanFileRow;
  private final Row scanStateRow;
  private final FileStatus addFileStatus;
  private final CloseableIterator<FilteredColumnarBatch> logicalRowDataColumnarBatchIter;
  private CloseableIterator<Row> rowIter = null;
  private Row current = null;
  private boolean closed = false;

  public DeltaKernelPartitionReader(InputPartition partition) {
    try {
      // Recreate an engine for executor-side IO
      this.engine = DefaultEngine.create(new Configuration());

      DeltaKernelScanPartition p = (DeltaKernelScanPartition) partition;
      this.scanFileRow =
          JsonUtils.rowFromJson(
              p.getSerializedScanFileRow(), InternalScanFileUtils.SCAN_FILE_SCHEMA);
      this.scanStateRow = JsonUtils.rowFromJson(p.getSerializedScanState(), getScanStateSchema());
      this.addFileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);

      CloseableIterator<ColumnarBatch> physicalRowDataIter =
          this.engine
              .getParquetHandler()
              .readParquetFiles(
                  Utils.singletonCloseableIterator(addFileStatus),
                  ScanStateRow.getPhysicalDataReadSchema(this.engine, scanStateRow),
                  java.util.Optional.empty())
              .map(x -> x.getData());

      this.logicalRowDataColumnarBatchIter =
          io.delta.kernel.Scan.transformPhysicalData(
              this.engine, scanStateRow, scanFileRow, physicalRowDataIter);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private io.delta.kernel.types.StructType getScanStateSchema() {
    try {
      java.lang.reflect.Field f = ScanStateRow.class.getDeclaredField("SCHEMA");
      f.setAccessible(true);
      return (io.delta.kernel.types.StructType) f.get(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean next() {
    if (closed) return false;

    if (rowIter != null && rowIter.hasNext()) {
      current = rowIter.next();
      return true;
    }
    if (logicalRowDataColumnarBatchIter.hasNext()) {
      rowIter = logicalRowDataColumnarBatchIter.next().getRows();
      return next();
    }
    return false;
  }

  @Override
  public InternalRow get() {
    return new KernelRowToSparkRowWrapper(current);
  }

  @Override
  public void close() {
    if (!closed) {
      try {
        logicalRowDataColumnarBatchIter.close();
      } catch (IOException ignored) {
      }
      if (rowIter != null) {
        try {
          rowIter.close();
        } catch (IOException ignored) {
        }
      }
      closed = true;
    }
  }
}
