/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.delta.flink.Conf;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.expressions.Literal;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.checkerframework.checker.nullness.qual.Nullable;
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
public class DeltaSinkWriter
    implements CommittingSinkWriter<RowData, DeltaWriterResult>,
        RemovalListener<Map<String, String>, DeltaWriterTask> {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaSinkWriter.class);

  private final String jobId;
  private final int subtaskId;
  private final int attemptNumber;

  private final DeltaTable deltaTable;
  private final DeltaSinkConf conf;

  private final Cache<Map<String, String>, DeltaWriterTask> writerTasksByPartition;

  private List<DeltaWriterResult> completedWrites;

  private final SinkWriterMetricGroup metricGroup;

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
    this.writerTasksByPartition =
        Caffeine.newBuilder()
            .executor(Runnable::run)
            .maximumSize(Conf.getInstance().getSinkWriterNumConcurrentFiles())
            .removalListener(this)
            .build();
    this.completedWrites = new ArrayList<>();

    this.metricGroup = metricGroup;
    metricGroup.gauge(
        "resultBufferSize",
        () ->
            this.writerTasksByPartition.asMap().values().stream()
                .map(DeltaWriterTask::getResultBuffer)
                .mapToLong(List::size)
                .sum());
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

    Map<String, String> writerKey =
        partitionValues.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));

    writerTasksByPartition
        .get(
            writerKey,
            (key) ->
                new DeltaWriterTask(
                    jobId, subtaskId, attemptNumber, deltaTable, conf, partitionValues))
        .write(element, context);

    // Recording Metrics
    if (element instanceof BinaryRowData) {
      this.metricGroup.getNumBytesSendCounter().inc(((BinaryRowData) element).getSizeInBytes());
    }
    this.metricGroup.getNumRecordsSendCounter().inc();
  }

  @Override
  public Collection<DeltaWriterResult> prepareCommit() {
    LOG.debug("Preparing commits");

    writerTasksByPartition.invalidateAll();

    List<DeltaWriterResult> results = List.copyOf(completedWrites);
    completedWrites.clear();
    return results;
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

  @Override
  public void onRemoval(
      @Nullable Map<String, String> key, @Nullable DeltaWriterTask value, RemovalCause cause) {
    // Close the evicted task and collect its result
    try {
      if (value != null) {
        completedWrites.addAll(value.complete());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
