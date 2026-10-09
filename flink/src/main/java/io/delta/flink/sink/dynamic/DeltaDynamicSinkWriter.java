/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.flink.sink.dynamic;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.delta.flink.Conf;
import io.delta.flink.sink.Conversions;
import io.delta.flink.sink.DeltaSinkConf;
import io.delta.flink.sink.DeltaWriterResult;
import io.delta.flink.sink.DeltaWriterTask;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.*;
import java.io.IOException;
import java.util.*;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A multi-table Delta writer that dynamically routes each incoming {@link DynamicRow} to the
 * appropriate Delta table based on its table name and partition values.
 *
 * <p>Tables are resolved on demand via a {@link DeltaTableProvider} and held in an LRU cache to
 * bound memory usage when writing to 4000+ concurrent tables. Writer tasks are similarly cached per
 * (table, partition) key and flushed on eviction.
 *
 * <p>At each checkpoint, all open writer tasks are flushed and the accumulated {@link
 * DeltaDynamicWriterResult}s are returned, one per (table, partition) combination.
 *
 * <p>Partition {@link Literal}s for Delta writes are taken from {@link DynamicRow#getRow()} via
 * {@link Conversions.FlinkToDelta#partitionValues}; {@link DynamicRow#getPartitionValues()} is used
 * only for Flink routing and writer-cache keys (aligned with {@link PartitionKeySelector}).
 */
public class DeltaDynamicSinkWriter
    implements CommittingSinkWriter<DynamicRow, DeltaDynamicWriterResult>,
        RemovalListener<DeltaDynamicSinkWriter.WriterKey, DeltaWriterTask> {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaDynamicSinkWriter.class);

  /** Same convention as {@link PartitionKeySelector}: null routing partition values => none. */
  private static final String[] EMPTY_ROUTING_PARTITION_VALUES = new String[0];

  private final String jobId;
  private final int subtaskId;
  private final int attemptNumber;

  private final DeltaTableProvider tableProvider;
  private final Map<String, String> conf;

  /** LRU cache of loaded tables, keyed by table name. Evicted tables are closed. */
  private final Cache<String, TableState> tableCache;

  /**
   * LRU cache of active writer tasks, keyed by (tableName, partitionValues). Evicted tasks are
   * flushed and their results collected.
   */
  private final Cache<WriterKey, DeltaWriterTask> writerTaskCache;

  private final List<DeltaDynamicWriterResult> completedWrites;

  private final Counter tableEvictionCounter;

  private DeltaDynamicSinkWriter(
      String jobId,
      int subtaskId,
      int attemptNumber,
      DeltaTableProvider tableProvider,
      Map<String, String> conf,
      SinkWriterMetricGroup metricGroup) {
    this.jobId = jobId;
    this.subtaskId = subtaskId;
    this.attemptNumber = attemptNumber;
    this.tableProvider = tableProvider;
    this.conf = conf;
    this.completedWrites = new ArrayList<>();
    this.tableEvictionCounter = metricGroup.addGroup("dynamic").counter("tableEvictionCount");

    this.tableCache =
        Caffeine.newBuilder()
            .executor(Runnable::run)
            .maximumSize(Conf.getInstance().getDynamicSinkMaxCachedTables())
            .removalListener(
                (String key, TableState state, RemovalCause cause) -> {
                  if (state != null) {
                    LOG.debug("Evicting table from cache: {}", key);
                    tableEvictionCounter.inc();
                    try {
                      state.table.close();
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  }
                })
            .build();

    this.writerTaskCache =
        Caffeine.newBuilder()
            .executor(Runnable::run)
            .maximumSize(Conf.getInstance().getSinkWriterNumConcurrentFiles())
            .removalListener(this)
            .build();

    metricGroup.addGroup("dynamic").gauge("activeTableCount", tableCache::estimatedSize);
    metricGroup.addGroup("dynamic").gauge("activeWriterTaskCount", writerTaskCache::estimatedSize);
  }

  @Override
  public void write(DynamicRow element, Context context) throws IOException, InterruptedException {
    String tableName = element.getTableName();

    TableState state =
        tableCache.get(
            tableName,
            k -> {
              StructType schema = DataTypeJsonSerDe.deserializeStructType(element.getSchemaStr());
              DeltaTable table =
                  tableProvider.getOrCreate(
                      element.getTableName(), schema, Arrays.asList(element.getPartitionColumns()));
              return new TableState(table, conf);
            });

    List<String> partitionColumns = state.table.getPartitionColumns();
    String[] routingPartitionValues = element.getPartitionValues();
    if (routingPartitionValues == null) {
      routingPartitionValues = EMPTY_ROUTING_PARTITION_VALUES;
    }
    if (routingPartitionValues.length != partitionColumns.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Partition value count (%d) does not match partition column count (%d) for table"
                  + " with columns %s",
              routingPartitionValues.length, partitionColumns.size(), partitionColumns));
    }

    Map<String, String> writerKeyMap = new LinkedHashMap<>(partitionColumns.size());
    for (int i = 0; i < partitionColumns.size(); i++) {
      writerKeyMap.put(partitionColumns.get(i), routingPartitionValues[i]);
    }
    WriterKey writerKey = new WriterKey(tableName, writerKeyMap);

    Map<String, Literal> partitionValues =
        Conversions.FlinkToDelta.partitionValues(
            state.table.getSchema(), partitionColumns, element.getRow());

    writerTaskCache
        .get(
            writerKey,
            k ->
                new DeltaWriterTask(
                    jobId, subtaskId, attemptNumber, state.table, state.conf, partitionValues))
        .write(element.getRow(), context);
  }

  @Override
  public Collection<DeltaDynamicWriterResult> prepareCommit() {
    LOG.debug("Preparing commits across all active tables");
    writerTaskCache.invalidateAll();

    List<DeltaDynamicWriterResult> results = List.copyOf(completedWrites);
    completedWrites.clear();
    return results;
  }

  @Override
  public void flush(boolean endOfInput) {}

  @Override
  public void writeWatermark(Watermark watermark) {}

  @Override
  public void close() throws Exception {
    LOG.debug("Closing DeltaDynamicSinkWriter");
    tableCache.invalidateAll();
  }

  /**
   * Called when a writer task is evicted from the LRU cache. Flushes the task and collects its
   * results, tagging each with the associated table name.
   */
  @Override
  public void onRemoval(
      @Nullable WriterKey key, @Nullable DeltaWriterTask task, RemovalCause cause) {
    if (task == null || key == null) {
      return;
    }
    try {
      List<DeltaWriterResult> results = task.complete();
      for (DeltaWriterResult result : results) {
        completedWrites.add(
            new DeltaDynamicWriterResult(
                key.tableName, result.getDeltaActions(), result.getContext()));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // ---------------------------------------------------------------------------
  // Supporting types
  // ---------------------------------------------------------------------------

  /**
   * Holds a loaded {@link DeltaTable} and its associated {@link DeltaSinkConf}, created once per
   * table on first encounter and evicted together from the table LRU cache.
   */
  static final class TableState {
    final DeltaTable table;
    final DeltaSinkConf conf;

    TableState(DeltaTable table, Map<String, String> globalConf) {
      this.table = table;
      this.conf = new DeltaSinkConf(table.getSchema(), globalConf);
    }
  }

  /**
   * Composite LRU cache key combining a table name and its partition values.
   *
   * <p>Uses value-based equality so that structurally identical keys hit the same cache entry.
   */
  public static final class WriterKey {
    final String tableName;
    final Map<String, String> partitionValues;

    WriterKey(String tableName, Map<String, String> partitionValues) {
      this.tableName = tableName;
      this.partitionValues = partitionValues;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof WriterKey)) return false;
      WriterKey that = (WriterKey) o;
      return Objects.equals(tableName, that.tableName)
          && Objects.equals(partitionValues, that.partitionValues);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableName, partitionValues);
    }

    @Override
    public String toString() {
      return "TablePartitionKey{tableName='"
          + tableName
          + "', partitionValues="
          + partitionValues
          + "}";
    }
  }

  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  public static class Builder {

    private String jobId;
    private int subtaskId;
    private int attemptNumber;
    private DeltaTableProvider tableProvider;
    private Map<String, String> conf;
    private SinkWriterMetricGroup metricGroup;

    public Builder() {}

    public Builder withJobId(String jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder withSubtaskId(int subtaskId) {
      this.subtaskId = subtaskId;
      return this;
    }

    public Builder withAttemptNumber(int attemptNumber) {
      this.attemptNumber = attemptNumber;
      return this;
    }

    public Builder withTableProvider(DeltaTableProvider tableProvider) {
      this.tableProvider = tableProvider;
      return this;
    }

    public Builder withConf(Map<String, String> conf) {
      this.conf = conf;
      return this;
    }

    public Builder withMetricGroup(SinkWriterMetricGroup metricGroup) {
      this.metricGroup = metricGroup;
      return this;
    }

    public DeltaDynamicSinkWriter build() {
      Objects.requireNonNull(jobId, "jobId must not be null");
      Objects.requireNonNull(tableProvider, "tableProvider must not be null");
      Objects.requireNonNull(metricGroup, "metricGroup must not be null");
      if (conf == null) {
        conf = Map.of();
      }
      return new DeltaDynamicSinkWriter(
          jobId, subtaskId, attemptNumber, tableProvider, conf, metricGroup);
    }
  }
}
