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
import io.delta.flink.Conf;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits {@link DeltaDynamicCommittable}s to their respective Delta tables.
 *
 * <p>A single committer instance may receive committables for multiple tables (those whose table
 * key hashes to this subtask). Tables are loaded on demand via a {@link DeltaTableProvider} and
 * held in an LRU cache to bound memory usage.
 *
 * <p>For each batch of committables, this committer:
 *
 * <ol>
 *   <li>Groups by table name,
 *   <li>Within each group, sorts by checkpoint ID to preserve ordering,
 *   <li>Commits each checkpoint's actions atomically to the corresponding Delta table.
 * </ol>
 *
 * <p>Commit idempotency is guaranteed by the Delta transaction identifier: replayed committables
 * produced by Flink recovery will be deduplicated by the Delta log.
 */
public class DeltaDynamicCommitter implements Committer<DeltaDynamicCommittable> {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaDynamicCommitter.class);

  private final String jobId;
  private final int taskIndex;

  private final DeltaTableProvider tableProvider;

  /** LRU cache of open DeltaTable instances, keyed by table name. */
  private final Cache<String, DeltaTable> tableCache;

  private final Counter commitCounter;

  private DeltaDynamicCommitter(
      String jobId,
      int taskIndex,
      DeltaTableProvider tableProvider,
      SinkCommitterMetricGroup metricGroup) {
    this.jobId = jobId;
    this.taskIndex = taskIndex;
    this.tableProvider = tableProvider;

    this.tableCache =
        Caffeine.newBuilder()
            .executor(Runnable::run)
            .maximumSize(Conf.getInstance().getDynamicSinkMaxCachedTables())
            .removalListener(
                (String key,
                    DeltaTable table,
                    com.github.benmanes.caffeine.cache.RemovalCause cause) -> {
                  if (table != null) {
                    LOG.debug("Evicting table from committer cache: {}", key);
                    try {
                      table.close();
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  }
                })
            .build();

    metricGroup.addGroup("dynamic").gauge("activeTableCount", () -> tableCache.estimatedSize());
    this.commitCounter = metricGroup.addGroup("dynamic").counter("commitCount");
  }

  @Override
  public void commit(Collection<CommitRequest<DeltaDynamicCommittable>> committables)
      throws IOException, InterruptedException {
    LOG.debug("Starting commit for {} committables", committables.size());

    committables.stream()
        .collect(
            Collectors.groupingBy(req -> req.getCommittable().getTableName(), Collectors.toList()))
        .forEach(this::commitForTable);
  }

  @Override
  public void close() throws Exception {
    LOG.debug("Closing DeltaDynamicCommitter");
    tableCache.invalidateAll();
  }

  private void commitForTable(
      String tableName, List<CommitRequest<DeltaDynamicCommittable>> requests) {
    if (requests.isEmpty()) {
      return;
    }

    requests.sort(Comparator.comparingLong(r -> r.getCommittable().getCheckpointId()));

    requests.stream()
        .collect(
            Collectors.groupingBy(
                req -> req.getCommittable().getCheckpointId(), TreeMap::new, Collectors.toList()))
        .forEach(
            (checkpointId, checkpointRequests) ->
                commitForCheckpoint(tableName, checkpointId, checkpointRequests));
  }

  private void commitForCheckpoint(
      String tableName, long checkpointId, List<CommitRequest<DeltaDynamicCommittable>> requests) {
    LOG.debug(
        "Committing {} committables for table '{}' on checkpoint {}",
        requests.size(),
        tableName,
        checkpointId);

    DeltaDynamicCommittable first = requests.get(0).getCommittable();
    DeltaTable table =
        tableCache.get(
            tableName,
            k ->
                tableProvider.getOrCreate(
                    first.getTableName(),
                    null /* schema not needed; table already exists */,
                    Collections.emptyList()));

    table.refresh();

    String committerId = String.format("%s-%d-%s", jobId, taskIndex, tableName);

    long[] watermarks =
        requests.stream()
            .map(CommitRequest::getCommittable)
            .map(DeltaDynamicCommittable::getContext)
            .map(ctx -> new long[] {ctx.getLowWatermark(), ctx.getHighWatermark()})
            .reduce(
                new long[] {Long.MAX_VALUE, -1L},
                (a, b) -> {
                  a[0] = Math.min(a[0], b[0]);
                  a[1] = Math.max(a[1], b[1]);
                  return a;
                },
                (a, b) -> {
                  a[0] = Math.min(a[0], b[0]);
                  a[1] = Math.max(a[1], b[1]);
                  return a;
                });

    final CloseableIterable<Row> dataActions =
        new CloseableIterable<Row>() {
          @Override
          public CloseableIterator<Row> iterator() {
            return Utils.toCloseableIterator(
                requests.stream()
                    .flatMap(req -> req.getCommittable().getDeltaActions().stream())
                    .iterator());
          }

          @Override
          public void close() throws IOException {}
        };

    table.commit(
        dataActions,
        committerId,
        checkpointId,
        Map.of(
            "flink.low-watermark", String.valueOf(watermarks[0]),
            "flink.high-watermark", String.valueOf(watermarks[1])));

    commitCounter.inc();
  }

  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  public static final class Builder {
    private String jobId;
    private int taskIndex;
    private DeltaTableProvider tableProvider;
    private SinkCommitterMetricGroup metricGroup;

    public Builder() {}

    public Builder withJobId(String jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder withTaskIndex(int taskIndex) {
      this.taskIndex = taskIndex;
      return this;
    }

    public Builder withTableProvider(DeltaTableProvider tableProvider) {
      this.tableProvider = tableProvider;
      return this;
    }

    public Builder withMetricGroup(SinkCommitterMetricGroup metricGroup) {
      this.metricGroup = metricGroup;
      return this;
    }

    public DeltaDynamicCommitter build() {
      Objects.requireNonNull(jobId, "jobId must not be null");
      Objects.requireNonNull(tableProvider, "tableProvider must not be null");
      Objects.requireNonNull(metricGroup, "metricGroup must not be null");
      return new DeltaDynamicCommitter(jobId, taskIndex, tableProvider, metricGroup);
    }
  }
}
