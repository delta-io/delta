package io.delta.flink.sink;

import io.delta.flink.DeltaTable;
import io.delta.flink.table.CCv2KernelTable;
import io.delta.flink.table.FileSystemKernelTable;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.net.URI;
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

  private final DeltaTable deltaTable;

  public DeltaSink(DeltaTable deltaTable) {
    this.deltaTable = deltaTable;
  }

  @Override
  public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
    return new DeltaSinkWriter.Builder()
        .withJobId(context.getJobInfo().getJobId().toString())
        .withSubtaskId(context.getTaskInfo().getIndexOfThisSubtask())
        .withAttemptNumber(context.getTaskInfo().getAttemptNumber())
        .withDeltaTable(deltaTable)
        .withMetricGroup(context.metricGroup())
        .build();
  }

  @Override
  public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
    return new DeltaSinkWriter.Builder()
        .withJobId(context.getJobInfo().getJobId().toString())
        .withSubtaskId(context.getTaskInfo().getIndexOfThisSubtask())
        .withAttemptNumber(context.getTaskInfo().getAttemptNumber())
        .withDeltaTable(deltaTable)
        .withMetricGroup(context.metricGroup())
        .build();
  }

  @Override
  public Committer<DeltaCommittable> createCommitter(CommitterInitContext context)
      throws IOException {
    return new DeltaCommitter.Builder()
        .withJobId(context.getJobInfo().getJobId().toString())
        .withDeltaTable(deltaTable)
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
                Conversions.FlinkToDelta.partitionValues(
                        deltaTable.getSchema(), deltaTable.getPartitionColumns(), value)
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
    String uid = String.format("DeltaSink preCommit aggregator: %s", deltaTable.getId());
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
    private DeltaTable deltaTable;
    // For file-based tables
    private String tablePath;
    private RowType flinkSchema;
    private List<String> partitionColNames;
    // For catalog-based tables
    private String tableId;
    private String catalogEndpoint;
    private String catalogToken;

    public Builder withDeltaTable(DeltaTable deltaTable) {
      this.deltaTable = deltaTable;
      return this;
    }

    // For file-based tables
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

    // For catalog-based tables
    public Builder withTableId(String tableId) {
      this.tableId = tableId;
      return this;
    }

    public Builder withCatalogEndpoint(String catalogEndpoint) {
      this.catalogEndpoint = catalogEndpoint;
      return this;
    }

    public Builder withCatalogToken(String catalogToken) {
      this.catalogToken = catalogToken;
      return this;
    }

    public DeltaSink build() {
      if (deltaTable == null) {
        // Can use only one from tablePath or tableId
        Preconditions.checkArgument((tablePath != null) ^ (tableId != null),
                "Use either tablePath or tableId");
        if( tablePath != null ) {
          // File-based table
          StructType tableSchema = null;
          if (flinkSchema != null) {
            tableSchema = Conversions.FlinkToDelta.schema(flinkSchema);
          }
          deltaTable = new FileSystemKernelTable(
                  URI.create(tablePath), tableSchema, partitionColNames);
        } else {
          // Catalog-based table
          Objects.requireNonNull(catalogEndpoint);
          Objects.requireNonNull(catalogToken);
          deltaTable = new CCv2KernelTable(tableId,
                  Map.of(CCv2KernelTable.CATALOG_ENDPOINT, catalogEndpoint,
                          CCv2KernelTable.CATALOG_TOKEN, catalogToken));
        }
      }
      return new DeltaSink(deltaTable);
    }
  }
}
