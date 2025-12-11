package io.delta.flink.sink;

import io.delta.flink.DeltaTable;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * The Committer is responsible for committing the data staged by the CommittingSinkWriter in the
 * second step of a two-phase commit protocol.
 *
 * <p>A commit must be idempotent: If some failure occurs in Flink during commit phase, Flink will
 * restart from the last successful checkpoint and re-attempt to commit all committables. There are
 * two cases of failures:
 *
 * <ol>
 *   <li>Flink fails before completing checkpoint N. In this case, Flink discards all committables
 *       related to checkpoint N, and restart from reading the rows after checkpoint N-1. Flink
 *       calls the writer and committer to re-create the committables. Old committables are simply
 *       discarded. As the changes in checkpoint N is not written to Delta table, no special
 *       handling is needed in DeltaSink/DeltaCommitter.
 *   <li>Flink fails after completing checkpoint N. In this case, the changes in checkpoint N has
 *       been written to the Delta table. Flink will load committables from the persisted checkpoint
 *       N and replay them. This will cause the changes in checkpoint N to be inserted twice into
 *       Delta table as duplicated add files. We rely on Delta to auto dedup these duplicated add
 *       files. See @link{io.delta.kernel.TransactionBuilder::withTransactionId}
 * </ol>
 *
 * NOTE: Unlike IcebergCommitter, which writes the checkpoint ID into snapshot to prevent a data
 * file from being added twice to the table, DeltaCommitter relies on Delta protocol to handle
 * duplicated files. Thus we don't explicitly write jobId/checkpointId into DeltaLog.
 */
public class DeltaCommitter implements Committer<DeltaCommittable> {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaCommitter.class);

  // All committables should have the same job id as the committer.
  // For simplicity, we get the job id from constructor.
  private String jobId;
  private DeltaTable deltaTable;

  private SinkCommitterMetricGroup metricGroup;

  private DeltaCommitter(
          String jobId,
          DeltaTable deltaTable,
          SinkCommitterMetricGroup metricGroup) {
    this.jobId = jobId;
    this.deltaTable = deltaTable;
    this.metricGroup = metricGroup;
  }

  @Override
  public void commit(Collection<CommitRequest<DeltaCommittable>> committables)
      throws IOException, InterruptedException {
    LOG.debug("Starting commit");
    sortCommittablesByCheckpointId(committables).forEach(this::commitForSingleCheckpointId);
  }

  @Override
  public void close() throws Exception {}

  private void commitForSingleCheckpointId(
      long checkpointId, List<CommitRequest<DeltaCommittable>> committables) {
    LOG.debug("Committing {} committables on checkpoint {}", committables.size(), checkpointId);

    final CloseableIterable<Row> dataActions =
        new CloseableIterable<Row>() {
          @Override
          public CloseableIterator<Row> iterator() {
            return Utils.toCloseableIterator(
                committables.stream()
                    .flatMap(req -> req.getCommittable().getDeltaActions().stream())
                    .iterator());
          }

          @Override
          public void close() throws IOException {
            // Nothing to close
          }
        };

    deltaTable.commit(dataActions);
  }

  private TreeMap<Long, List<CommitRequest<DeltaCommittable>>> sortCommittablesByCheckpointId(
      Collection<CommitRequest<DeltaCommittable>> committables) {
    return committables.stream()
        .collect(
            Collectors.groupingBy(
                commitRequest -> commitRequest.getCommittable().getCheckpointId(),
                TreeMap::new,
                Collectors.toList()));
  }

  public static final class Builder {
    private String jobId;
    private DeltaTable deltaTable;
    private SinkCommitterMetricGroup metricGroup;

    public Builder() {}

    public Builder withJobId(String jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder withDeltaTable(DeltaTable deltaTable) {
      this.deltaTable = deltaTable;
      return this;
    }

    public Builder withMetricGroup(SinkCommitterMetricGroup metricGroup) {
      this.metricGroup = metricGroup;
      return this;
    }

    public DeltaCommitter build() {
      Objects.requireNonNull(jobId, "jobId must not be null");
      Objects.requireNonNull(deltaTable, "tableLoader must not be null");
      Objects.requireNonNull(metricGroup, "metricGroup must not be null");

      return new DeltaCommitter(jobId, deltaTable, metricGroup);
    }
  }
}
