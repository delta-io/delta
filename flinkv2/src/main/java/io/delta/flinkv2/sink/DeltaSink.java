package io.delta.flinkv2.sink;

import io.delta.flinkv2.utils.DataUtils;
import io.delta.flinkv2.utils.SchemaUtils;
import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.StructType;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Runs in its own subtask in a single thread.
 */
public class DeltaSink implements Sink<RowData>,
    SupportsCommitter<DeltaCommittable>,
    SupportsWriterState<RowData, DeltaSinkWriterState>,
    SupportsPreCommitTopology<DeltaCommittable, DeltaCommittable>,
    SupportsPreWriteTopology<RowData>,
    SupportsPostCommitTopology<DeltaCommittable>,
    Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaSink.class);

    ///////////////////////
    // Public static API //
    ///////////////////////

    public static DeltaSink forRowData(final String tablePath, final RowType rowType, final List<String> userProvidedPartitionColumns) {
        return new DeltaSink(tablePath, rowType, userProvidedPartitionColumns);
    }

    /////////////////////////////////////////
    // Member fields. MUST be Serializable //
    /////////////////////////////////////////

    private final String appId; // TODO: be able to restore this
    private final String tablePath;
    private final RowType writeOperatorFlinkSchema;
    private final List<String> userProvidedPartitionColumns;
    private final String mockTxnStateJson;
    private Set<String> tablePartitionColumns; // non-final since we set it in try-catch block

    private DeltaSink(final String tablePath, final RowType rowType, final List<String> userProvidedPartitionColumns) {
        this.appId = java.util.UUID.randomUUID().toString();
        this.tablePath = tablePath;
        this.writeOperatorFlinkSchema = rowType;
        this.userProvidedPartitionColumns = userProvidedPartitionColumns;

        LOG.info(
            String.format(
                "Scott > DeltaSink > constructor :: tablePath=%s, appId=%s, rowType=%s, userProvidedPartitionColumns=%s",
                tablePath,
                appId,
                rowType,
                userProvidedPartitionColumns)
        );

        LOG.info(
            String.format("Equivalent Delta schema is: %s", SchemaUtils.toDeltaDataType(rowType))
        );

        final StructType writeOperatorDeltaSchema = SchemaUtils.toDeltaDataType(rowType);
        final Engine engine = DefaultEngine.create(new Configuration());
        final Table table = Table.forPath(engine, tablePath);
        TransactionBuilder mockTxnBuilder =
            table.createTransactionBuilder(
                engine,
                "FlinkV2",
                Operation.MANUAL_UPDATE // this doesn't matter, we aren't committing anything
            );

        try {

            final Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            final StructType tableSchema = latestSnapshot.getSchema(engine);

            if (!tableSchema.equivalent(writeOperatorDeltaSchema)) {
                throw new RuntimeException(
                    String.format(
                        "Table Schema does not match Write Operator Delta Schema.\nTable schema: %s\nWrite Operator Delta schema: %s",
                        tableSchema,
                        writeOperatorDeltaSchema
                    )
                );
            }
            this.tablePartitionColumns = ((SnapshotImpl) latestSnapshot).getMetadata().getPartitionColNames();
        } catch (TableNotFoundException ex) {
            // table doesn't exist
            LOG.info(String.format("Scott > DeltaSink > constructor :: DOES NOT EXIST tablePath=%s", tablePath));
            this.tablePartitionColumns = new HashSet<>(userProvidedPartitionColumns);

            mockTxnBuilder = mockTxnBuilder
                .withSchema(engine, writeOperatorDeltaSchema)
                .withPartitionColumns(engine, userProvidedPartitionColumns);
        }

        final Transaction mockTxn = mockTxnBuilder.build(engine);
        final Row mockTxnState = mockTxn.getTransactionState(engine);

        this.mockTxnStateJson = JsonUtils.rowToJson(mockTxnState);
    }

    /////////////////
    // Public APIs //
    /////////////////

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
        LOG.info(
            String.format(
                "Scott > DeltaSink > createWriter :: context.getRestoredCheckpointId()=%s",
                context.getRestoredCheckpointId()
            )
        );
        return DeltaSinkWriter.createNewWriter(appId, mockTxnStateJson, writeOperatorFlinkSchema, context.getRestoredCheckpointId());
    }

    @Override
    public Committer<DeltaCommittable> createCommitter(CommitterInitContext context) throws IOException {
        LOG.info(
            String.format(
                "Scott > DeltaSink > createCommitter :: context.getRestoredCheckpointId=%s",
                context.getRestoredCheckpointId()
            )
        );
        return new DeltaCommitter(appId, tablePath, writeOperatorFlinkSchema, userProvidedPartitionColumns, context.getRestoredCheckpointId());
    }

    /**
     * This method ensures that all rows with the same partitionHash will be sent to the same
     * {@link DeltaSinkWriter}. It makes no promises about how many unique partitionHash's that
     * a {@link DeltaSinkWriter} will handle (it may even be 0).
     */
    @Override
    public DataStream<RowData> addPreWriteTopology(DataStream<RowData> inputDataStream) {
        LOG.info("Scott > DeltaSink > addPreWriteTopology");

//        return inputDataStream;

        return inputDataStream.keyBy(new KeySelector<RowData, Integer>() {
            @Override
            public Integer getKey(RowData value) throws Exception {
                return DataUtils.flinkRowToPartitionValues(writeOperatorFlinkSchema, value, tablePartitionColumns).hashCode();
            }
        });
    }

        @Override
    public DataStream<CommittableMessage<DeltaCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<DeltaCommittable>> committables) {
        LOG.info("Scott > DeltaSink > addPreCommitTopology");
        // Sets the partitioning of the DataStream so that the output values all go to the first
        // instance of the next processing operator
        //
        // This essentially is what gives us a global committer.
        // TODO: consider injecting checkpointId information explicitly? like in the iceberg PR?
        // CommittableMessage::getCheckpointId
        return committables.global();
    }

    @Override
    public SimpleVersionedSerializer<DeltaCommittable> getWriteResultSerializer() {
        LOG.info("Scott > DeltaSink > getWriteResultSerializer");
        return new DeltaCommittable.Serializer();
    }

    @Override
    public SimpleVersionedSerializer<DeltaCommittable> getCommittableSerializer() {
        LOG.info("Scott > DeltaSink > getCommittableSerializer");
        return new DeltaCommittable.Serializer();
    }

    //////////////////////////////
    // SupportsWriterState APIs //
    //////////////////////////////

    @Override
    public StatefulSinkWriter<RowData, DeltaSinkWriterState> restoreWriter(
            WriterInitContext context, Collection<DeltaSinkWriterState> recoveredState)
            throws IOException {
        // TODO: re-calculate the mockTxnStateJson (i.e. rebase on the latest table version)

        Set<String> writerIds = recoveredState.stream()
            .map(DeltaSinkWriterState::getWriterId)
            .collect(Collectors.toSet());

        if (writerIds.size() != 1) {
            String msg = String.format("ERROR: restoreWriter called with # writerIds != 1. writerIds=%s", writerIds);
            LOG.info(msg);
            throw new RuntimeException(msg);
        }

        DeltaSinkWriterState state = recoveredState.stream().findFirst().get();

        LOG.info(String.format("Scott > DeltaSink > restoreWriter :: writerId=%s, context.getRestoredCheckpointId=%s, writerStateNextCheckpointId=%s", state.getWriterId(), context.getRestoredCheckpointId(), state.getCheckpointId()));

        return DeltaSinkWriter.restoreWriter(state.getAppId(), state.getWriterId(), mockTxnStateJson, writeOperatorFlinkSchema, context.getRestoredCheckpointId());
    }

    @Override
    public SimpleVersionedSerializer<DeltaSinkWriterState> getWriterStateSerializer() {
        LOG.info("Scott > DeltaSink > getWriterStateSerializer");
        return new DeltaSinkWriterState.Serializer();
    }

    @Override
    public void addPostCommitTopology(DataStream<CommittableMessage<DeltaCommittable>> committables) {
        committables.global().process(new PostCommitOperator());
    }
}
