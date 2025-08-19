package io.delta.spark.dsv2.scan.batch;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.connector.read.InputPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared context for Kernel-based Spark scans that manages scan state and partition planning.
 * Caches FilteredColumnarBatch data and converts them to Spark InputPartitions on demand.
 */
public class KernelSparkScanContext {

  private static final Logger LOG = LoggerFactory.getLogger(KernelSparkScanContext.class);

  private final Scan kernelScan;
  private final Engine engine;
  private final String serializedScanState;
  private Optional<List<FilteredColumnarBatch>> cachedBatches;

  public KernelSparkScanContext(Scan kernelScan, Engine engine) {
    this.kernelScan = requireNonNull(kernelScan, "kernelScan is null");
    this.engine = requireNonNull(engine, "engine is null");
    this.serializedScanState = JsonUtils.rowToJson(kernelScan.getScanState(engine));
    this.cachedBatches = Optional.empty();
  }

  public synchronized InputPartition[] planPartitions() {
    if (!cachedBatches.isPresent()) {
      List<FilteredColumnarBatch> batches = new ArrayList<>();
      try (CloseableIterator<FilteredColumnarBatch> scanFiles = kernelScan.getScanFiles(engine)) {
        while (scanFiles.hasNext()) {
          FilteredColumnarBatch batch = scanFiles.next();
          batches.add(batch);
        }
      } catch (IOException e) {
        LOG.error("Failed to read scan files from kernel scan", e);
        throw new UncheckedIOException("Failed to read scan files from kernel scan", e);
      }
      cachedBatches = Optional.of(batches);
    }

    // Create InputPartitions from cached batches
    List<InputPartition> partitions = new ArrayList<>();
    for (FilteredColumnarBatch batch : cachedBatches.get()) {
      try (CloseableIterator<Row> rows = batch.getRows()) {
        while (rows.hasNext()) {
          Row row = rows.next();
          partitions.add(
              new KernelSparkInputPartition(serializedScanState, JsonUtils.rowToJson(row)));
        }
      } catch (IOException e) {
        LOG.error("Failed to read rows from filtered columnar batch", e);
        throw new UncheckedIOException("Failed to read rows from filtered columnar batch", e);
      }
    }

    return partitions.toArray(new InputPartition[0]);
  }
}
