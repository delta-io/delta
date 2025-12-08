package io.delta.flink.sink;

import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private Engine engine;
  private Table table;
  private final Row committerContext;

  private SinkCommitterMetricGroup metricGroup;

  private boolean creatingNewTable;

  private DeltaCommitter(
      String jobId,
      Engine engine,
      Table table,
      Row committerContext,
      SinkCommitterMetricGroup metricGroup) {
    this.jobId = jobId;
    this.engine = engine;
    this.table = table;
    this.committerContext = committerContext;

    this.metricGroup = metricGroup;
  }

  @Override
  public void commit(Collection<CommitRequest<DeltaCommittable>> committables)
      throws IOException, InterruptedException {
    LOG.debug("Starting commit");
    try {
      table.getLatestSnapshot(engine);
      creatingNewTable = false;
    } catch (TableNotFoundException e) {
      creatingNewTable = true;
    }
    sortCommittablesByCheckpointId(committables).forEach(this::commitForSingleCheckpointId);
  }

  @Override
  public void close() throws Exception {}

  private void commitForSingleCheckpointId(
      long checkpointId, List<CommitRequest<DeltaCommittable>> committables) {
    LOG.debug("Committing {} committables on checkpoint {}", committables.size(), checkpointId);

    TransactionBuilder txnBuilder =
        table
            .createTransactionBuilder(
                engine,
                "DeltaSink/Kernel",
                creatingNewTable ? Operation.CREATE_TABLE : Operation.WRITE)
            .withTransactionId(engine, jobId, checkpointId);

    if (creatingNewTable) {
      // For a new table set the table schema in the transaction builder
      txnBuilder =
          txnBuilder
              .withSchema(engine, TransactionStateRow.getLogicalSchema(committerContext))
              .withPartitionColumns(
                  engine, TransactionStateRow.getPartitionColumnsList(committerContext));
    }
    final Transaction txn = txnBuilder.build(engine);

    // We check the table's latest schema is still the same as committer schema.
    // The check is delayed here to detect external modification to the table schema.
    if (!creatingNewTable) {
      final Snapshot readSnapshot = table.getSnapshotAsOfVersion(engine, txn.getReadTableVersion());
      final StructType tableSchema = txn.getSchema(engine);
      final StructType committerSchema = TransactionStateRow.getLogicalSchema(committerContext);
      Preconditions.checkArgument(
          readSnapshot.getPath().equals(TransactionStateRow.getTablePath(this.committerContext)),
          String.format(
              "Committer path does not match the latest table path."
                  + "Table path: %s, Committer path: %s",
              readSnapshot.getPath(), TransactionStateRow.getTablePath(this.committerContext)));
      Preconditions.checkArgument(
          committerSchema.equivalent(tableSchema),
          String.format(
              "DeltaSink does not support schema evolution. "
                  + "Table schema: %s, Committer schema: %s",
              tableSchema, committerSchema));
    }

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

    txn.commit(engine, dataActions);
    creatingNewTable = false;
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
    private Engine engine;
    private Table table;
    private Row committerContext;
    private SinkCommitterMetricGroup metricGroup;

    public Builder() {}

    public Builder withJobId(String jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder withEngine(Engine engine) {
      this.engine = engine;
      return this;
    }

    public Builder withTable(Table table) {
      this.table = table;
      return this;
    }

    public Builder withCommitterContext(Row committerContext) {
      this.committerContext = committerContext;
      return this;
    }

    public Builder withCommitterContext(String committerContextJson) {
      this.committerContext =
          JsonUtils.rowFromJson(committerContextJson, TransactionStateRow.SCHEMA);
      return this;
    }

    public Builder withMetricGroup(SinkCommitterMetricGroup metricGroup) {
      this.metricGroup = metricGroup;
      return this;
    }

    public DeltaCommitter build() {
      Objects.requireNonNull(jobId, "jobId must not be null");
      Objects.requireNonNull(engine, "engine must not be null");
      Objects.requireNonNull(table, "table must not be null");
      Objects.requireNonNull(committerContext, "committerContext must not be null");
      Objects.requireNonNull(metricGroup, "metricGroup must not be null");

      return new DeltaCommitter(jobId, engine, table, committerContext, metricGroup);
    }
  }
}
