package io.delta.flink.sink;

import io.delta.flink.DeltaTable;
import io.delta.kernel.expressions.Literal;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delta Writer implementation of the Flink V2 Connector API. Writes out the data to the final
 * place, and emits {@link DeltaWriterResult} at every checkpoint for add actions created by this
 * writer.
 */
public class DeltaSinkWriter implements CommittingSinkWriter<RowData, DeltaWriterResult> {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaSinkWriter.class);

  private final String jobId;
  private final int subtaskId;
  private final int attemptNumber;

  private final DeltaTable deltaTable;

  private final Map<Map<String, String>, DeltaWriterTask> writerTasksByPartition;

  private final SinkWriterMetricGroup metricGroup;
  private final Counter elementCounter;

  private DeltaSinkWriter(
      String jobId,
      int subtaskId,
      int attemptNumber,
      DeltaTable deltaTable,
      SinkWriterMetricGroup metricGroup) {
    this.jobId = jobId;
    this.subtaskId = subtaskId;
    this.attemptNumber = attemptNumber;

    this.deltaTable = deltaTable;
    this.writerTasksByPartition = new HashMap<>();

    this.metricGroup = metricGroup;
    this.elementCounter = metricGroup.counter("elementCounter");
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
            .collect(
                Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().toString()));

    if (!writerTasksByPartition.containsKey(writerKey)) {
      writerTasksByPartition.put(
          writerKey,
          new DeltaWriterTask(jobId, subtaskId, attemptNumber, deltaTable, partitionValues));
    }
    writerTasksByPartition.get(writerKey).write(element, context);
    elementCounter.inc();
  }

  @Override
  public Collection<DeltaWriterResult> prepareCommit() throws IOException, InterruptedException {
    LOG.debug("Preparing commits");
    final Collection<DeltaWriterResult> output = new ArrayList<>();

    for (DeltaWriterTask writerTask : writerTasksByPartition.values()) {
      output.addAll(writerTask.complete());
    }
    writerTasksByPartition.clear();
    return output;
  }

  @Override
  public void flush(boolean endOfInput) throws IOException, InterruptedException {}

  @Override
  public void close() throws Exception {}

  public static class Builder {

    private String jobId;
    private int subtaskId;
    private int attemptNumber;

    private DeltaTable deltaTable;

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

    public Builder withMetricGroup(SinkWriterMetricGroup metricGroup) {
      this.metricGroup = metricGroup;
      return this;
    }

    public DeltaSinkWriter build() {
      // Optional safety checks
      Objects.requireNonNull(jobId, "jobId must not be null");
      Objects.requireNonNull(deltaTable, "deltaTable must not be null");
      Objects.requireNonNull(metricGroup, "metricGroup must not be null");

      return new DeltaSinkWriter(jobId, subtaskId, attemptNumber, deltaTable, metricGroup);
    }
  }
}
