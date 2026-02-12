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
package io.delta.spark.internal.v2.read;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;
import org.apache.spark.sql.Dataset;

/**
 * Kernel {@link Scan} implementation that composes the V2 components via interfaces.
 *
 * <p>Delegates execution to a {@link ScanExecutor} (Component 3) and metadata queries to a Kernel
 * delegate scan (for {@code getScanState} / {@code getRemainingFilter}).
 *
 * <h3>Effective Java design (Items 17, 18, 64):</h3>
 *
 * <ul>
 *   <li>Composition over inheritance (Item 18) — wraps {@link ScanExecutor}.
 *   <li>Refers to components by interface (Item 64) — typed as {@link ScanExecutor}.
 *   <li>Immutable (Item 17) — all fields {@code final}.
 *   <li>{@code final} class (Item 19).
 * </ul>
 *
 * @see ScanExecutor Component 3: Executor (interface, wrapped here)
 */
public final class DistributedScan implements Scan {

  private final ScanExecutor executor; // Item 64: typed by interface
  private final Scan delegateScan; // for getScanState / getRemainingFilter

  /**
   * Creates a DistributedScan.
   *
   * @param plannedDataFrame filtered DataFrame from Planner (non-null)
   * @param snapshot kernel snapshot for delegation metadata (non-null)
   * @param readSchema read schema for Kernel delegation (non-null)
   */
  DistributedScan(
      Dataset<org.apache.spark.sql.Row> plannedDataFrame,
      Snapshot snapshot,
      StructType readSchema) {
    requireNonNull(plannedDataFrame, "plannedDataFrame"); // Item 49
    requireNonNull(snapshot, "snapshot");
    requireNonNull(readSchema, "readSchema");
    this.executor = new SparkRowScanExecutor(plannedDataFrame);
    this.delegateScan = snapshot.getScanBuilder().withReadSchema(readSchema).build();
  }

  @Override
  public CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine) {
    return executor.execute();
  }

  @Override
  public Optional<Predicate> getRemainingFilter() {
    return delegateScan.getRemainingFilter();
  }

  @Override
  public Row getScanState(Engine engine) {
    return delegateScan.getScanState(engine);
  }
}
