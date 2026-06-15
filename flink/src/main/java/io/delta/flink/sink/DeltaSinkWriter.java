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

package io.delta.flink.sink;

import io.delta.flink.sink.mergestrategy.Upsert;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.*;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Delta writer implementation based on Flink’s Sink V2 Connector API.
 *
 * <p>This writer is responsible for writing incoming records to the target Delta table storage and
 * producing {@link DeltaWriterResult} objects that describe the data written by this writer since
 * the last successful checkpoint.
 *
 * <p>At each checkpoint, the writer emits a {@code DeltaWriterResult} containing the Delta {@code
 * AddFile} actions (and any other relevant actions) generated during that checkpoint interval.
 * These results are subsequently aggregated and committed by the downstream committer components to
 * create a new Delta table version.
 *
 * <p>This implementation follows Flink’s checkpointing and fault-tolerance model:
 *
 * <ul>
 *   <li>Writes are buffered and tracked per checkpoint,
 *   <li>{@code DeltaWriterResult}s are emitted during checkpoint preparation, and
 *   <li>Commit responsibility is delegated to the committer to ensure correctness and exactly-once
 *       or at-least-once semantics.
 * </ul>
 *
 * <p>The writer does not perform table commits directly. Instead, it focuses solely on producing
 * durable data files and describing their effects via {@code DeltaWriterResult}, allowing commit
 * coordination and deduplication to be handled centrally.
 */
public class DeltaSinkWriter implements CommittingSinkWriter<RowData, DeltaWriterResult> {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaSinkWriter.class);

  private final String jobId;
  private final int subtaskId;
  private final int attemptNumber;

  private final DeltaTable deltaTable;
  private final DeltaSinkConf conf;

  private final SinkWriterMetricGroup metricGroup;
  private volatile long lastSendTimeMs;

  /**
   * Strategy that owns per-checkpoint upsert/delete bookkeeping and turns it into Delta actions.
   * Selected once at construction from {@link DeltaSinkConf#getWriteMode()}.
   */
  private final MergeStrategy mergeStrategy;

  private DeltaSinkWriter(
      String jobId,
      int subtaskId,
      int attemptNumber,
      DeltaTable deltaTable,
      DeltaSinkConf conf,
      SinkWriterMetricGroup metricGroup) {
    this.jobId = jobId;
    this.subtaskId = subtaskId;
    this.attemptNumber = attemptNumber;

    this.deltaTable = deltaTable;
    this.conf = conf;

    this.metricGroup = metricGroup;
    metricGroup.setCurrentSendTimeGauge(() -> lastSendTimeMs);

    this.mergeStrategy = conf.createMergeStrategy();
    this.mergeStrategy.init(this);
    LOG.debug(
        "DeltaSinkWriter created in {} mode (primary-key ordinals = {})",
        conf.getWriteMode(),
        Arrays.toString(conf.getPrimaryKeyOrdinals()));
  }

  /**
   * {@link DeltaSink} implements {@link SupportsPreWriteTopology} and its {@link
   * DeltaSink#addPreCommitTopology} method ensures that all rows with the same partition hash will
   * be sent to the same {@link DeltaSinkWriter} instance.
   *
   * <p>However, a single {@link DeltaSinkWriter} instance may receive rows for more than one
   * partition hash. It may also receive no rows at all.
   */
  @Override
  public void write(RowData element, Context context) throws IOException, InterruptedException {
    final Map<String, Literal> partitionValues =
        Conversions.FlinkToDelta.partitionValues(
            deltaTable.getSchema(), deltaTable.getPartitionColumns(), element);
    // "Trust the provided RowKind" upsert policy:
    //   - INSERT is taken at face value: the source claims this PK is new, so we just append.
    //     This keeps the hot path cheap for INSERT-heavy workloads (e.g. CDC bootstrap).
    //     Trade-off: if the source can redeliver an INSERT for a PK already in the table
    //     across a checkpoint boundary (operator-induced source replay, CDC re-snapshot,
    //     at-least-once source), the sink will produce duplicate rows for that PK. Flink's
    //     own failover within a checkpoint is still safe via the transactional committer.
    //   - UPDATE_AFTER carries a new image for an existing key. We record the PK so the
    //     merge step removes the pre-image, then fall through to the INSERT case to append
    //     the new image as a regular AddFile.
    //   - UPDATE_BEFORE conveys no information the matching UPDATE_AFTER doesn't already
    //     carry, so we drop it. Flink elides it for PK sinks anyway.
    //   - DELETE records the PK; the merge step emits the corresponding RemoveFile/DV
    //     without appending a row.
    switch (element.getRowKind()) {
      case INSERT:
        mergeStrategy.insert(extractPrimaryKey(element), partitionValues, element, context);
        break;
      case UPDATE_AFTER:
        mergeStrategy.upsert(extractPrimaryKey(element), partitionValues, element, context);
        break;
      case UPDATE_BEFORE:
        // Dropped — see policy comment above.
        break;
      case DELETE:
        mergeStrategy.delete(extractPrimaryKey(element), partitionValues);
        break;
      default:
        // Defensive: if Flink ever introduces a new RowKind, we'd rather fail loudly than
        // silently treat the row as a no-op while still incrementing the metric counters.
        throw new IllegalStateException("Unexpected RowKind: " + element.getRowKind());
    }

    // Recording Metrics
    if (element instanceof BinaryRowData) {
      this.metricGroup.getNumBytesSendCounter().inc(((BinaryRowData) element).getSizeInBytes());
    }
    this.metricGroup.getNumRecordsSendCounter().inc();
  }

  public DeltaTable getTable() {
    return deltaTable;
  }

  public DeltaSinkConf getConf() {
    return conf;
  }

  /**
   * Factory for the writer task that backs a single partition in the current checkpoint. Upsert
   * mode needs the dedup-aware {@link DeltaUpsertWriterTask}; append mode uses the plain {@link
   * DeltaWriterTask}.
   */
  public DeltaWriterTask newWriterTask(Map<String, Literal> partitionValues) {
    if (mergeStrategy instanceof Upsert) {
      return new DeltaUpsertWriterTask(
          jobId,
          subtaskId,
          attemptNumber,
          deltaTable,
          conf,
          partitionValues,
          (Upsert) mergeStrategy);
    }
    return new DeltaWriterTask(jobId, subtaskId, attemptNumber, deltaTable, conf, partitionValues);
  }

  /**
   * Extracts the primary-key values of {@code row} as a {@code List<Object>} in PK column order.
   *
   * <p>Uses {@link RowData#isNullAt} + {@link RowData}'s typed accessors so primitive types are
   * boxed without going through the more expensive generic field access path.
   */
  private List<Literal> extractPrimaryKey(RowData row) {
    StructType schema = conf.getSinkSchema();
    int[] ordinals = conf.getPrimaryKeyOrdinals();
    List<Literal> key = new ArrayList<>(ordinals.length);
    for (int ord : ordinals) {
      key.add(Conversions.FlinkToDelta.data(schema, row, ord));
    }
    return key;
  }

  /**
   * Delegates to {@link MergeStrategy#merge()} to flush all pending writer tasks and materialize
   * any upsert/delete bookkeeping into Delta actions for the current checkpoint. Bumps the {@code
   * numFilesWritten} counter by the number of resulting writes — accounting at this layer keeps the
   * metric a writer concern instead of threading the metric group through every {@link
   * MergeStrategy}.
   */
  @Override
  public Collection<DeltaWriterResult> prepareCommit() {
    LOG.debug("Preparing commits");
    try {
      Collection<DeltaWriterResult> results = mergeStrategy.merge();
      metricGroup.counter("numFilesWritten").inc(results.size());
      return results;
    } catch (IOException e) {
      throw new RuntimeException("merge failed for checkpoint", e);
    }
  }

  @Override
  public void flush(boolean endOfInput) {}

  @Override
  public void writeWatermark(Watermark watermark) {
    // Do nothing, watermark is queried using context
  }

  @Override
  public void close() throws Exception {
    // close the DeltaTable will interrupt ongoing operations such as log-replay
    LOG.debug("Force closing the Writer. Interrupting running table loading");
    this.deltaTable.close();
  }

  public static class Builder {

    private String jobId;
    private int subtaskId;
    private int attemptNumber;

    private DeltaTable deltaTable;
    private DeltaSinkConf conf;

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

    public Builder withDeltaTable(DeltaTable deltaTable) {
      this.deltaTable = deltaTable;
      return this;
    }

    public Builder withConf(DeltaSinkConf conf) {
      this.conf = conf;
      return this;
    }

    public Builder withMetricGroup(SinkWriterMetricGroup metricGroup) {
      this.metricGroup = metricGroup;
      return this;
    }

    public DeltaSinkWriter build() {
      // Optional safety checks
      Objects.requireNonNull(jobId, "jobId must not be null");
      Objects.requireNonNull(deltaTable, "deltaTable must not be null");
      Objects.requireNonNull(metricGroup, "metricGroup must not be null");
      Objects.requireNonNull(conf, "conf must not be null");

      return new DeltaSinkWriter(jobId, subtaskId, attemptNumber, deltaTable, conf, metricGroup);
    }
  }
}
