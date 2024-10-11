/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.delta.table;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.commons.compress.utils.Sets;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;

abstract class SimpleScan<ThisT extends SimpleScan<ThisT>> implements BatchScan {
  private final Table table;
  private final Map<String, String> options;
  private Long snapshotId = null;
  private Schema baseSchema = null;
  private Collection<String> selection = null;
  private Schema projection = null;
  private Expression filter = Expressions.alwaysTrue();
  private boolean caseSensitive = false;
  private boolean includeColumnStats = false;
  private boolean ignoreResiduals = false;
  private ExecutorService planExecutor = null;
  private MetricsReporter reporter = null;

  SimpleScan(Table table) {
    this.table = table;
    this.baseSchema = table.schema();
    this.options = Maps.newHashMap();
  }

  protected SimpleScan(SimpleScan<ThisT> toCopy) {
    this.table = toCopy.table;
    this.snapshotId = toCopy.snapshotId;
    this.baseSchema = toCopy.baseSchema;
    this.selection = toCopy.selection;
    this.projection = toCopy.projection;
    this.filter = toCopy.filter;
    this.options = Maps.newHashMap(toCopy.options);
    this.caseSensitive = toCopy.caseSensitive;
    this.includeColumnStats = toCopy.includeColumnStats;
    this.ignoreResiduals = toCopy.ignoreResiduals;
    this.planExecutor = toCopy.planExecutor;
    this.reporter = toCopy.reporter;
  }

  protected abstract ThisT self();

  protected abstract ThisT copy(ThisT toCopy);

  protected void sendScanEvent() {
    Listeners.notifyAll(new ScanEvent(table.name(), snapshotId, filter, schema()));
  }

  /**
   * Creates a copy of the current scan and passes the copy to the consumer before returning it.
   *
   * <p>This is used to implement a simple refinement pattern without a context object.
   *
   * @param consumer a lambda that can modify the copy before returning it
   * @return a copy of this scan with modifications from the consumer lambda
   */
  private ThisT withChanges(Consumer<SimpleScan<ThisT>> consumer) {
    ThisT scan = copy(self());
    consumer.accept(scan);
    return scan;
  }

  // refinement method implementations for the Iceberg Scan API

  @Override
  public BatchScan useSnapshot(long snapshotId) {
    return withChanges(
        scan -> {
          scan.snapshotId = snapshotId;
          scan.baseSchema = SnapshotUtil.schemaFor(table, snapshotId);
        });
  }

  @Override
  public BatchScan useRef(String name) {
    return withChanges(
        scan -> {
          Snapshot snap = table.snapshot(name);
          Preconditions.checkArgument(snap != null, "Cannot find ref %s", name);
          scan.snapshotId = snap.snapshotId();
          scan.baseSchema = SnapshotUtil.schemaFor(table, name);
        });
  }

  @Override
  public BatchScan asOfTime(long timestampMillis) {
    return withChanges(
        scan -> {
          scan.snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, timestampMillis);
          scan.baseSchema = SnapshotUtil.schemaFor(table, scan.snapshotId);
        });
  }

  @Override
  public BatchScan select(Collection<String> columns) {
    return withChanges(scan -> scan.selection = columns);
  }

  @Override
  public BatchScan project(Schema schema) {
    return withChanges(scan -> scan.projection = schema);
  }

  @Override
  public BatchScan filter(Expression expr) {
    return withChanges(scan -> scan.filter = Expressions.and(scan.filter, expr));
  }

  @Override
  public BatchScan caseSensitive(boolean caseSensitive) {
    return withChanges(scan -> scan.caseSensitive = caseSensitive);
  }

  @Override
  public BatchScan includeColumnStats() {
    return withChanges(scan -> scan.includeColumnStats = true);
  }

  @Override
  public BatchScan ignoreResiduals() {
    return withChanges(scan -> scan.ignoreResiduals = true);
  }

  @Override
  public BatchScan option(String property, String value) {
    return withChanges(scan -> scan.options.put(property, value));
  }

  @Override
  public BatchScan planWith(ExecutorService executorService) {
    return withChanges(scan -> scan.planExecutor = executorService);
  }

  @Override
  public BatchScan metricsReporter(MetricsReporter reporter) {
    return withChanges(scan -> scan.reporter = reporter);
  }

  // accessors for accumulated configuration

  @Override
  public Table table() {
    return table;
  }

  @Override
  public Snapshot snapshot() {
    return snapshotId != null ? table.snapshot(snapshotId) : table.currentSnapshot();
  }

  @Override
  public Schema schema() {
    if (selection != null) {
      Set<Integer> requiredFieldIds = Sets.newHashSet();

      // all filter columns are required
      requiredFieldIds.addAll(
          Binder.boundReferences(
              baseSchema.asStruct(), Collections.singletonList(filter), caseSensitive));

      // all projection columns are required
      Set<Integer> selectedIds;
      if (caseSensitive) {
        selectedIds = TypeUtil.getProjectedIds(baseSchema.select(selection));
      } else {
        selectedIds = TypeUtil.getProjectedIds(baseSchema.caseInsensitiveSelect(selection));
      }
      requiredFieldIds.addAll(selectedIds);

      return TypeUtil.project(baseSchema, requiredFieldIds);

    } else if (projection != null) {
      return projection;
    }

    return baseSchema;
  }

  @Override
  public Expression filter() {
    return filter;
  }

  @Override
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  @Override
  public long targetSplitSize() {
    long tableValue =
        PropertyUtil.propertyAsLong(
            table.properties(), TableProperties.SPLIT_SIZE, TableProperties.SPLIT_SIZE_DEFAULT);
    return PropertyUtil.propertyAsLong(options, TableProperties.SPLIT_SIZE, tableValue);
  }

  @Override
  public int splitLookback() {
    int tableValue =
        PropertyUtil.propertyAsInt(
            table.properties(),
            TableProperties.SPLIT_LOOKBACK,
            TableProperties.SPLIT_LOOKBACK_DEFAULT);
    return PropertyUtil.propertyAsInt(options, TableProperties.SPLIT_LOOKBACK, tableValue);
  }

  @Override
  public long splitOpenFileCost() {
    long tableValue =
        PropertyUtil.propertyAsLong(
            table.properties(),
            TableProperties.SPLIT_OPEN_FILE_COST,
            TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
    return PropertyUtil.propertyAsLong(options, TableProperties.SPLIT_OPEN_FILE_COST, tableValue);
  }

  @Override
  public CloseableIterable<ScanTaskGroup<ScanTask>> planTasks() {
    return TableScanUtil.planTaskGroups(
        planFiles(), targetSplitSize(), splitLookback(), splitOpenFileCost());
  }

  // accessors for child implementations

  protected long snapshotId() {
    return snapshotId;
  }

  protected ExecutorService planExecutor() {
    return planExecutor;
  }

  protected MetricsReporter reporter() {
    return reporter;
  }

  protected boolean shouldIgnoreResiduals() {
    return ignoreResiduals;
  }

  protected boolean shouldIncludeColumnStats() {
    return includeColumnStats;
  }
}
