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
package io.delta.spark.dsv2.scan;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.dsv2.scan.batch.KernelSparkInputPartition;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
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
  private final AtomicReference<Optional<List<FilteredColumnarBatch>>> cachedBatches;

  public KernelSparkScanContext(Scan kernelScan, Engine engine) {
    this.kernelScan = requireNonNull(kernelScan, "kernelScan is null");
    this.engine = requireNonNull(engine, "engine is null");
    this.serializedScanState = JsonUtils.rowToJson(kernelScan.getScanState(engine));
    this.cachedBatches = new AtomicReference<>(Optional.empty());
  }

  /**
   * Plans and creates input partitions for Spark execution.
   * 
   * <p>This method converts Delta Kernel scan files into Spark InputPartitions. Each file row
   * from the kernel scan creates one input partition containing serialized scan state and file
   * metadata.
   * 
   * <p>Results are cached on first invocation using thread-safe lazy initialization. Subsequent
   * calls return the partitions calculated based on the same cached batches..
   * 
   * @return array of InputPartitions for Spark to process
   * @throws UncheckedIOException if kernel scan fails or partition creation fails
   */
  public InputPartition[] planPartitions() {
    Optional<List<FilteredColumnarBatch>> batches = cachedBatches.get();
    if (!batches.isPresent()) {
      synchronized (this) {
        batches = cachedBatches.get();
        if (!batches.isPresent()) {
          List<FilteredColumnarBatch> batchList = new ArrayList<>();
          try (CloseableIterator<FilteredColumnarBatch> scanFiles =
              kernelScan.getScanFiles(engine)) {
            while (scanFiles.hasNext()) {
              FilteredColumnarBatch batch = scanFiles.next();
              batchList.add(batch);
            }
          } catch (IOException e) {
            LOG.warn("Failed to read from kernel scan", e);
            throw new UncheckedIOException("Failed to read added files from kernel scan", e);
          }
          batches = Optional.of(batchList);
          cachedBatches.set(batches);
        }
      }
    }

    // Create InputPartitions from cached batches
    // Implementation note: not caching partitions for the future extension of runtime filtering.
    // where it is generated and push to scan after the first planInputPartition.
    List<InputPartition> partitions = new ArrayList<>();
    for (FilteredColumnarBatch batch : batches.get()) {
      try (CloseableIterator<Row> rows = batch.getRows()) {
        while (rows.hasNext()) {
          Row row = rows.next();
          partitions.add(
              new KernelSparkInputPartition(serializedScanState, JsonUtils.rowToJson(row)));
        }
      } catch (IOException e) {
        LOG.warn("Failed to construct input partition", e);
        throw new UncheckedIOException("Failed to construct input partition", e);
      }
    }

    return partitions.toArray(new InputPartition[0]);
  }
}
