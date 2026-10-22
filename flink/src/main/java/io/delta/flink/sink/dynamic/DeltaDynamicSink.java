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

import java.util.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.*;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Flink v2 sink that dynamically routes incoming {@link DynamicRow}s to multiple Delta tables.
 *
 * <p>Supports writing to 4000+ concurrent tables within a single Flink job by combining:
 *
 * <ul>
 *   <li>LRU-cached table resolution via {@link DeltaTableProvider},
 *   <li>per-table Parquet file generation in {@link DeltaDynamicSinkWriter},
 *   <li>parallel pre-commit aggregation keyed by table name, and
 *   <li><strong>multiple {@link DeltaDynamicCommitter} instances</strong> — Flink runs one
 *       committer object per committer subtask; how many subtasks (and thus committers) is
 *       determined by the <em>sink parallelism the job configures</em> (e.g. {@code
 *       sinkTo(...).setParallelism(n)}, Table API sink parallelism, or environment defaults), not
 *       by hard-coding parallelism inside this sink. Each subtask commits a disjoint subset of
 *       tables (by key hash) while per-table ordering is preserved on one subtask.
 * </ul>
 *
 * <p>The job graph looks like below:
 *
 * <pre>{@code
 *                              Flink sink
 * +--------------------------------------------------------------------------------------------+
 * |                                                                                            |
 * | +----------+                                                                               |
 * | | writer 1 | \                                                                             |
 * | +----------+  \  DeltaDynamicWriterResults                                                 |
 * |                \                                                                           |
 * | +----------+    \ +-------------------------------+    +-----------------------------+     |
 * | | writer 2 | ---> | Pre-commit aggregators         |    | Committer subtasks           |     |
 * | +----------+      | keyed by table name           | -> | (user sink parallelism);     |     |
 * |                /  | one committable / table / CP    |    | each DeltaDynamicCommitter   |     |
 * | +----------+  /                                                                           |
 * | | writer N | /                                                                            |
 * | +----------+                                                                               |
 * +--------------------------------------------------------------------------------------------+
 * }</pre>
 *
 * <p>Pre-commit aggregation is keyed by table name; operator parallelisms for writers, pre-commit,
 * and committers follow <strong>Flink sink translation and the parallelism the application sets on
 * the sink</strong> (or env defaults). This class does not override pre-commit parallelism
 * explicitly. Flink sink v2 invokes {@link #createCommitter} once per committer subtask; tables are
 * spread across subtasks by key hash, so different tables may commit on different subtasks while
 * work for one table stays on one subtask.
 */
public class DeltaDynamicSink
    implements Sink<DynamicRow>,
        SupportsCommitter<DeltaDynamicCommittable>,
        SupportsPreCommitTopology<DeltaDynamicWriterResult, DeltaDynamicCommittable>,
        SupportsPreWriteTopology<DynamicRow>,
        SupportsPostCommitTopology<DeltaDynamicCommittable> {

  private final DeltaTableProvider tableProvider;
  private final Map<String, String> conf;

  private DeltaDynamicSink(DeltaTableProvider tableProvider, Map<String, String> conf) {
    this.tableProvider = tableProvider;
    this.conf = conf;
  }

  // Deprecated API for backward compatibility; will not be called.
  public SinkWriter<DynamicRow> createWriter(InitContext context) {
    throw new UnsupportedOperationException("Should not be called");
  }

  @Override
  public SinkWriter<DynamicRow> createWriter(WriterInitContext context) {
    return new DeltaDynamicSinkWriter.Builder()
        .withJobId(context.getJobInfo().getJobId().toString())
        .withSubtaskId(context.getTaskInfo().getIndexOfThisSubtask())
        .withAttemptNumber(context.getTaskInfo().getAttemptNumber())
        .withTableProvider(tableProvider)
        .withConf(conf)
        .withMetricGroup(context.metricGroup())
        .build();
  }

  @Override
  public Committer<DeltaDynamicCommittable> createCommitter(CommitterInitContext context) {
    return new DeltaDynamicCommitter.Builder()
        .withJobId(context.getJobInfo().getJobId().toString())
        .withTaskIndex(context.getTaskInfo().getIndexOfThisSubtask())
        .withTableProvider(tableProvider)
        .withMetricGroup(context.metricGroup())
        .build();
  }

  /**
   * Routes rows so that the same (table name, partition values) land on the same writer subtask.
   * This spreads load across writers by partition and bounds per-writer state similarly to {@link
   * io.delta.flink.sink.PartitionKeySelector} on the single-table sink.
   */
  @Override
  public DataStream<DynamicRow> addPreWriteTopology(DataStream<DynamicRow> inputDataStream) {
    return inputDataStream.keyBy(new PartitionKeySelector());
  }

  /**
   * Partitions writer committables by table name and aggregates them in parallel, emitting one
   * {@link DeltaDynamicCommittable} per table per checkpoint per subtask. Parallelism is not set
   * here so it follows the sink / environment configuration chosen by the application.
   */
  @Override
  public DataStream<CommittableMessage<DeltaDynamicCommittable>> addPreCommitTopology(
      DataStream<CommittableMessage<DeltaDynamicWriterResult>> writerResults) {
    TypeInformation<CommittableMessage<DeltaDynamicCommittable>> typeInfo =
        CommittableMessageTypeInfo.of(this::getCommittableSerializer);
    String uid = "DynamicPreCommit Agg";

    return writerResults
        .filter(new WriterResultWithLineageFilter())
        .keyBy(new TableKeySelector())
        .transform(uid, typeInfo, new DeltaDynamicWriterResultAggregator())
        .uid(uid);
  }

  @Override
  public SimpleVersionedSerializer<DeltaDynamicWriterResult> getWriteResultSerializer() {
    return new DeltaDynamicWriterResult.Serializer();
  }

  @Override
  public SimpleVersionedSerializer<DeltaDynamicCommittable> getCommittableSerializer() {
    return new DeltaDynamicCommittable.Serializer();
  }

  @Override
  public void addPostCommitTopology(
      DataStream<CommittableMessage<DeltaDynamicCommittable>> committables) {}

  public static Builder builder() {
    return new Builder();
  }

  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  public static class Builder {

    private DeltaTableProvider tableProvider;
    private Map<String, String> conf;

    private Builder() {
      this.conf = new HashMap<>();
    }

    /**
     * Sets the provider used to resolve or create Delta tables by name.
     *
     * @param tableProvider table provider
     * @return this builder
     */
    public Builder withTableProvider(DeltaTableProvider tableProvider) {
      this.tableProvider = tableProvider;
      return this;
    }

    /**
     * Sets global sink configurations (file rolling, schema evolution mode, etc.) applied to all
     * tables.
     *
     * @param conf configuration map
     * @return this builder
     */
    public Builder withConf(Map<String, String> conf) {
      this.conf = new HashMap<>(conf);
      return this;
    }

    public DeltaDynamicSink build() {
      Objects.requireNonNull(tableProvider, "tableProvider must not be null");
      return new DeltaDynamicSink(tableProvider, Collections.unmodifiableMap(conf));
    }
  }
}
