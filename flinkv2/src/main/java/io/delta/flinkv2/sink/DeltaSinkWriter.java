package io.delta.flinkv2.sink;

import io.delta.flinkv2.utils.DataUtils;
import io.delta.flinkv2.utils.SchemaUtils;
import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.StructType;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.*;

// TODO: implement stateful sink writer
public class DeltaSinkWriter implements CommittingSinkWriter<RowData, DeltaCommittable>,
    StatefulSinkWriter<RowData, DeltaSinkWriterState> {

    static boolean hasFailed = false;

    private long nextCheckpointId; // checkpointId that all FUTURE writes will be a part of
    private long lastSnapshottedCheckpointId = -1;
    private final String appId;
    private final Engine engine;
    private final String writerId;
    private final Row mockTxnState;
    private final RowType writeOperatorFlinkSchema;
    private final StructType writeOperatorDeltaSchema;
    private Set<String> tablePartitionColumns; // non-final since we set it in try-catch block

    private Map<Map<String, Literal>, DeltaSinkWriterTask> writerTasksByPartition;

    public static DeltaSinkWriter restoreWriter(String appId, String writerId, long checkpointId, String tablePath, RowType writeOperatorFlinkSchema, List<String> userProvidedPartitionColumns) {
        return new DeltaSinkWriter(appId, writerId, checkpointId, tablePath, writeOperatorFlinkSchema, userProvidedPartitionColumns);
    }

    public static DeltaSinkWriter createNewWriter(String appId, String tablePath, RowType writeOperatorFlinkSchema, List<String> userProvidedPartitionColumns) {
        return new DeltaSinkWriter(appId, java.util.UUID.randomUUID().toString(), 1, tablePath, writeOperatorFlinkSchema, userProvidedPartitionColumns);
    }

    private DeltaSinkWriter(String appId, String writerId, long nextCheckpointId, String tablePath, RowType writeOperatorFlinkSchema, List<String> userProvidedPartitionColumns) {
        this.appId = appId;
        this.writerId = writerId;
        this.nextCheckpointId = nextCheckpointId;
        this.engine = DefaultEngine.create(new Configuration());
        this.writeOperatorFlinkSchema = writeOperatorFlinkSchema;
        this.writeOperatorDeltaSchema = SchemaUtils.toDeltaDataType(writeOperatorFlinkSchema);
        this.writerTasksByPartition = new HashMap<>();

        final Table table = Table.forPath(engine, tablePath);
        TransactionBuilder txnBuilder =
            table.createTransactionBuilder(
                engine,
                "FlinkV2",
                Operation.MANUAL_UPDATE // this doesn't matter, we aren't committing anything
            );

        try {
            final Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            this.tablePartitionColumns = ((SnapshotImpl) latestSnapshot).getMetadata().getPartitionColNames();
        } catch (TableNotFoundException ex) {
            // table doesn't exist
            txnBuilder = txnBuilder
                .withSchema(engine, writeOperatorDeltaSchema)
                .withPartitionColumns(engine, userProvidedPartitionColumns);

            this.tablePartitionColumns = new HashSet<>(userProvidedPartitionColumns);
        }

        this.mockTxnState = txnBuilder.build(engine).getTransactionState(engine);

        System.out.println(
            String.format(
                "Scott > DeltaSinkWriter > constructor :: writerId=%s, checkpointId=%s, writeOperatorDeltaSchema=%s, txnStateSchema=%s",
                writerId,
                nextCheckpointId,
                writeOperatorDeltaSchema,
                mockTxnState.getSchema()
            )
        );
    }

    /////////////////
    // Public APIs //
    /////////////////

    /**
     * {@link DeltaSink} implements {@link SupportsPreWriteTopology} and its
     * {@link DeltaSink#addPreCommitTopology} method ensures that all rows with the same partition
     * hash will be sent to the same {@link DeltaSinkWriter} instance.
     * <p>
     * However, a single {@link DeltaSinkWriter} instance may receive rows for more than one
     * partition hash. It may also receive no rows at all.
     */
    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        // checkpointId is the checkpoint id that this data WILL be checkpointed into
        if (lastSnapshottedCheckpointId != -1 && nextCheckpointId != lastSnapshottedCheckpointId + 1) {
            System.out.println(
                String.format(
                    "Scott > DeltaSinkWriter[%s] > write :: weird state where checkpointId=%s but lastSnapshottedCheckpointId=%s",
                    writerId,
                    nextCheckpointId,
                    lastSnapshottedCheckpointId
                )
            );
        }

        if (!hasFailed && java.util.concurrent.ThreadLocalRandom.current().nextInt() % 400 == 0) {
            hasFailed = true;

            int totalRowsBuffered = writerTasksByPartition.values().stream()
                .mapToInt(DeltaSinkWriterTask::getBufferSize)
                .sum();
            final String msg = String.format("RANDOM FAILURE ---- Scott > DeltaSinkWriter[%s] > write :: element=%s :: nextCheckpointId=%s, lastSnapshottedCheckpointId=%s, totalRowsBuffered=%s", writerId, element, nextCheckpointId, lastSnapshottedCheckpointId, totalRowsBuffered);
            System.out.println(msg);
            throw new RuntimeException("!!!!!" + msg);
        }

        final Map<String, Literal> partitionValues =
            DataUtils.flinkRowToPartitionValues(writeOperatorFlinkSchema, element, tablePartitionColumns);

        if (!writerTasksByPartition.containsKey(partitionValues)) {
            writerTasksByPartition.put(
                partitionValues,
                new DeltaSinkWriterTask(
                    appId,
                    writerId,
                    nextCheckpointId,
                    engine,
                    partitionValues,
                    mockTxnState,
                    writeOperatorDeltaSchema,
                    writerId
                )
            );
        }

        writerTasksByPartition.get(partitionValues).write(element, context);

        System.out.println(
            String.format("Scott > DeltaSinkWriter[%s] > write :: element=%s", writerId, element)
        );
    }

    /**
     * Preparing the commit is the first part of a two-phase commit protocol.
     *
     * Returns the data to commit as the second step of the two-phase commit protocol.
     *
     * This gets called before every checkpoint, even if no data was written
     */
    @Override
    public Collection<DeltaCommittable> prepareCommit() throws IOException, InterruptedException {
        int totalRowsBuffered = writerTasksByPartition.values().stream()
            .mapToInt(DeltaSinkWriterTask::getBufferSize)
            .sum();
        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: checkpointId=%s, totalRowsBuffered=%s", writerId, nextCheckpointId, totalRowsBuffered));

        final Collection<DeltaCommittable> output = new ArrayList<>();

        for (DeltaSinkWriterTask writerTask : writerTasksByPartition.values()) {
            output.addAll(writerTask.prepareCommit());
        }

        writerTasksByPartition.clear();

        nextCheckpointId++;

        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: incrementing checkpointId to %s", writerId, nextCheckpointId));

        return output;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > flush", writerId));
    }

    @Override
    public void close() throws Exception {

    }

    /////////////////////////////
    // StatefulSinkWriter APIs //
    /////////////////////////////

    /** Called after prepareCommit */
    @Override
    public List<DeltaSinkWriterState> snapshotState(long latestCheckpointId) throws IOException {
        lastSnapshottedCheckpointId = latestCheckpointId;

        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > snapshotState :: checkpointIdToSnapshot=%s, nextCheckpointId=%s, ---- %s", writerId, latestCheckpointId, nextCheckpointId, this));

        // nextCheckpointId + 1 because:
        // we are checkpointing version N
        // nextCheckpointId is N + 1 (all future writes will be a part of checkpoint N + 1)
        // if, before checkpoint N + 1 is complete, there is a failure, then the pipeline will
        // restart, and the checkpointId of all future writes will be N + 2 (which is nextCheckpointId + 1)
        return Collections.singletonList(new DeltaSinkWriterState(appId, writerId, nextCheckpointId + 1));
    }

    @Override
    public String toString() {
        return "DeltaSinkWriter{" +
            "checkpointId=" + nextCheckpointId +
            ", appId='" + appId + '\'' +
            ", engine=" + engine +
            ", writerId='" + writerId + '\'' +
            ", numWriterTasks='" + writerTasksByPartition.size() + '\'' +
            '}';
    }
}
