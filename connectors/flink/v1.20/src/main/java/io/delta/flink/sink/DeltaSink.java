package io.delta.flink.sink;

import io.delta.kernel.*;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import jdk.jfr.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink v2 sink offer different hooks to insert custom topologies into the sink. We will use the
 * following:
 *
 * <ul>
 *   <li>{@link SupportsPreWriteTopology} which redistributes the data to the writers
 *   <li>{@link org.apache.flink.api.connector.sink2.SinkWriter} which writes data/delete files, and
 *       generates the {@link DeltaWriterResult} objects for the files
 *   <li>{@link SupportsPreCommitTopology} which we use to place the {@link
 *       DeltaWriterResultAggregator} which merges the individual {@link
 *       org.apache.flink.api.connector.sink2.SinkWriter}'s {@link DeltaWriterResult}s to a single
 *       {@link DeltaCommittable}
 *   <li>{@link DeltaCommitter} which commits the incoming{@link DeltaCommittable}s to the Iceberg
 *       table
 *   <li>{@link SupportsPostCommitTopology} we could use for incremental compaction later. This is
 *       not implemented yet.
 * </ul>
 *
 * <p>The job graph looks like below:
 *
 * <pre>{@code
 *                            Flink sink
 *               +-----------------------------------------------------------------------------------+
 *               |                                                                                   |
 * +-------+     | +----------+                               +-------------+      +---------------+ |
 * | Map 1 | ==> | | writer 1 |                               | committer 1 | ---> | post commit 1 | |
 * +-------+     | +----------+                               +-------------+      +---------------+ |
 *               |             \                             /                \                      |
 *               |        DeltaWriterResults        DeltaCommittables          \                     |
 *               |               \                         /                    \                    |
 * +-------+     | +----------+   \ +-------------------+ /                      \ +---------------+ |
 * | Map 2 | ==> | | writer 2 | --->| commit aggregator |                          | post commit 2 | |
 * +-------+     | +----------+     +-------------------+                          +---------------+ |
 *               |                                             Commit only on                        |
 *               |                                             a single committer                    |
 *               +-----------------------------------------------------------------------------------+
 * }</pre>
 */
@Experimental
public class DeltaSink
    implements Sink<RowData>,
        SupportsCommitter<DeltaCommittable>,
        SupportsPreCommitTopology<DeltaWriterResult, DeltaCommittable>,
        SupportsPreWriteTopology<RowData>,
        SupportsPostCommitTopology<DeltaCommittable> {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaSink.class);

  private final TableLoader tableLoader;
  private final RowType flinkSchema;
  private String writerContextJson;
  private List<String> partitionColumnNames;

  // Non-serializable fields
  private transient Engine engine;
  private transient Table table;

  public DeltaSink(
      TableLoader tableLoader, RowType flinkRowType, List<String> partitionColumnNames) {
    this.flinkSchema = flinkRowType;
    this.tableLoader = tableLoader;
    this.partitionColumnNames = partitionColumnNames;

    getEngine();
    getTable();
    try {
      initWithExistingTable();
    } catch (TableNotFoundException ignored) {
      initWithNewTable();
    }
  }

  protected void initWithExistingTable() {
    // With an existing table, partitions loaded from the table take precedence
    final Snapshot latestSnapshot = table.getLatestSnapshot(getEngine());
    this.partitionColumnNames = latestSnapshot.getPartitionColumnNames();

    final StructType tableSchema = latestSnapshot.getSchema();

    StructType flinkSchemaAsDelta = Conversions.FlinkToDelta.schema(flinkSchema);
    Preconditions.checkArgument(
        tableSchema.equivalent(flinkSchemaAsDelta),
        String.format(
            "DeltaSink does not support Schema Evolution.\nTable schema: %s\nFlink schema: %s",
            tableSchema, flinkSchemaAsDelta));
    // We use a temporary transaction to generate a TransactionStateRow.
    // It will be used as context for Writer and Committer.
    // The transaction will not be committed. It is discarded afterward.
    TransactionBuilder writerContextBuilder =
        table.createTransactionBuilder(getEngine(), "DeltaSink", Operation.MANUAL_UPDATE);
    final Transaction temporaryTxn = writerContextBuilder.build(getEngine());
    this.writerContextJson = JsonUtils.rowToJson(temporaryTxn.getTransactionState(getEngine()));
  }

  protected void initWithNewTable() {
    TransactionBuilder writerContextBuilder =
        table.createTransactionBuilder(
            getEngine(), "DeltaSink using Kernel", Operation.CREATE_TABLE);
    writerContextBuilder.withSchema(engine, Conversions.FlinkToDelta.schema(flinkSchema));
    if (partitionColumnNames != null) {
      writerContextBuilder.withPartitionColumns(engine, partitionColumnNames);
    }
    final Transaction temporaryTxn = writerContextBuilder.build(getEngine());
    this.writerContextJson = JsonUtils.rowToJson(temporaryTxn.getTransactionState(getEngine()));
  }

  protected Engine getEngine() {
    if (this.engine == null) {
      engine = tableLoader.getEngine();
    }
    return engine;
  }

  protected Table getTable() {
    if (table == null) {
      table = tableLoader.loadTable();
    }
    return table;
  }

  @Override
  public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
    return new DeltaSinkWriter.Builder()
        .withJobId(context.getJobInfo().getJobId().toString())
        .withSubtaskId(context.getTaskInfo().getIndexOfThisSubtask())
        .withAttemptNumber(context.getTaskInfo().getAttemptNumber())
        .withEngine(getEngine())
        .withWriterContext(writerContextJson)
        .withFlinkSchema(flinkSchema)
        .withMetricGroup(context.metricGroup())
        .build();
  }

  @Override
  public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
    return new DeltaSinkWriter.Builder()
        .withJobId(context.getJobInfo().getJobId().toString())
        .withSubtaskId(context.getTaskInfo().getIndexOfThisSubtask())
        .withAttemptNumber(context.getTaskInfo().getAttemptNumber())
        .withEngine(getEngine())
        .withWriterContext(writerContextJson)
        .withFlinkSchema(flinkSchema)
        .withMetricGroup(context.metricGroup())
        .build();
  }

  @Override
  public Committer<DeltaCommittable> createCommitter(CommitterInitContext context)
      throws IOException {
    return new DeltaCommitter.Builder()
        .withJobId(context.getJobInfo().getJobId().toString())
        .withEngine(getEngine())
        .withTable(getTable())
        .withCommitterContext(writerContextJson)
        .withMetricGroup(context.metricGroup())
        .build();
  }

  /**
   * This method ensures that all rows with the same partitionHash will be sent to the same {@link
   * DeltaSinkWriter}. It makes no promises about how many unique partitionHash's that a {@link
   * DeltaSinkWriter} will handle (it may even be 0).
   *
   * <p>TODO This design may cause imbalanced workload if the data distribution is skewed.
   */
  @Override
  public DataStream<RowData> addPreWriteTopology(DataStream<RowData> inputDataStream) {
    return inputDataStream.keyBy(
        (KeySelector<RowData, Integer>)
            value ->
                Conversions.FlinkToDelta.partitionValues(flinkSchema, value, partitionColumnNames)
                    .entrySet().stream()
                    .collect(
                        Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()))
                    .hashCode());
  }

  @Override
  public DataStream<CommittableMessage<DeltaCommittable>> addPreCommitTopology(
      DataStream<CommittableMessage<DeltaWriterResult>> writerResults) {
    TypeInformation<CommittableMessage<DeltaCommittable>> typeInformation =
        CommittableMessageTypeInfo.of(this::getCommittableSerializer);
    String uid = String.format("DeltaSink preCommit aggregator: %s", table.getPath(engine));
    // global forces all output records send to subtask 0 of the downstream committer operator.
    // This is to ensure commit only happen in one committer subtask.
    return writerResults
        .global()
        .transform(uid, typeInformation, new DeltaWriterResultAggregator())
        .uid(uid)
        .setParallelism(1)
        .setMaxParallelism(1)
        // global forces all output records send to subtask 0 of the downstream committer operator.
        // This is to ensure commit only happen in one committer subtask.
        // Once upstream Flink provides the capability of setting committer operator
        // parallelism to 1, this can be removed.
        .global();
  }

  @Override
  public SimpleVersionedSerializer<DeltaWriterResult> getWriteResultSerializer() {
    return new DeltaWriterResult.Serializer();
  }

  @Override
  public SimpleVersionedSerializer<DeltaCommittable> getCommittableSerializer() {
    return new DeltaCommittable.Serializer();
  }

  @Override
  public void addPostCommitTopology(DataStream<CommittableMessage<DeltaCommittable>> committables) {
    committables.global().process(new PostCommitOperator()).uid("DeltaSink postCommit processor");
  }

  public static class Builder {
    private TableLoader tableLoader;
    private String tablePath;
    private RowType flinkSchema;
    private List<String> partitionColNames;

    public Builder withTableLoader(TableLoader tableLoader) {
      this.tableLoader = tableLoader;
      return this;
    }

    public Builder withTablePath(String tablePath) {
      this.tablePath = tablePath;
      return this;
    }

    public Builder withFlinkSchema(RowType flinkSchema) {
      this.flinkSchema = flinkSchema;
      return this;
    }

    public Builder withPartitionColNames(List<String> partitionColNames) {
      this.partitionColNames = partitionColNames;
      return this;
    }

    public DeltaSink build() {
      Objects.requireNonNull(tablePath, "tablePath must not be null");
      Objects.requireNonNull(flinkSchema, "flinkSchema must not be null");

      if (tableLoader == null) {
        tableLoader = new TableLoader.PathBasedTableLoader(tablePath);
      }

      return new DeltaSink(tableLoader, flinkSchema, partitionColNames);
    }
  }
}
