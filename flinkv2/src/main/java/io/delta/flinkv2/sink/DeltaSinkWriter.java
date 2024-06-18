package io.delta.flinkv2.sink;

import io.delta.flinkv2.utils.DataUtils;
import io.delta.flinkv2.utils.SchemaUtils;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.types.StructType;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

// TODO: implement stateful sink writer
public class DeltaSinkWriter implements CommittingSinkWriter<RowData, DeltaCommittable>, StatefulSinkWriter<RowData, DeltaSinkWriterState> {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaSinkWriter.class);
    
    static int numWrittenElements = 0;
    static Set<RowData> writtenElements = new HashSet<>();

    static int delayCount = 0;
    static int failCount = 0;

    private long nextCheckpointId; // checkpointId that all FUTURE writes will be a part of
    private long lastSnapshottedCheckpointId = -1;
    private final String appId;
    private final Engine engine;
    private final String writerId;
    private final Row mockTxnState;
    private final RowType writeOperatorFlinkSchema;
    private final StructType writeOperatorDeltaSchema;
    private Set<String> tablePartitionColumns; // non-final since we set it in try-catch block
    private Set<RowData> elementsWrittenThisCheckpoint = new HashSet<>();

    private Map<Map<String, Literal>, DeltaSinkWriterTask> writerTasksByPartition;

    public static DeltaSinkWriter restoreWriter(String appId, String writerId, String mockTxnStateJson, RowType writeOperatorFlinkSchema, OptionalLong restoredCheckpointId) {
        return new DeltaSinkWriter(appId, writerId, mockTxnStateJson, writeOperatorFlinkSchema, restoredCheckpointId);
    }

    public static DeltaSinkWriter createNewWriter(String appId, String mockTxnStateJson, RowType writeOperatorFlinkSchema, OptionalLong restoredCheckpointId) {
        return new DeltaSinkWriter(appId, java.util.UUID.randomUUID().toString(), mockTxnStateJson, writeOperatorFlinkSchema, restoredCheckpointId);
    }

    private DeltaSinkWriter(String appId, String writerId, String mockTxnStateJson, RowType writeOperatorFlinkSchema, OptionalLong restoredCheckpointId) {
        if (restoredCheckpointId.isPresent()) {
            // restoredCheckpointId is the last successful checkpointId that was checkpointed and committed
            // thus, the *next* checkpointId is clearly 1 more than that
            this.nextCheckpointId = restoredCheckpointId.getAsLong() + 1;
        } else {
            this.nextCheckpointId = 1;
        }

        this.appId = appId;
        this.writerId = writerId;
        this.engine = DefaultEngine.create(new Configuration());
        this.writeOperatorFlinkSchema = writeOperatorFlinkSchema;
        this.writeOperatorDeltaSchema = SchemaUtils.toDeltaDataType(writeOperatorFlinkSchema);
        this.writerTasksByPartition = new HashMap<>();
        this.mockTxnState = JsonUtils.rowFromJson(mockTxnStateJson, TransactionStateRow.SCHEMA);
        this.tablePartitionColumns = new HashSet<>(TransactionStateRow.getPartitionColumnsList(mockTxnState));

        LOG.info(
            String.format(
                "Scott > DeltaSinkWriter > constructor :: writerId=%s, nextCheckpointId=%s, restoredCheckpointId=%s, writeOperatorDeltaSchema=%s, txnStateSchema=%s",
                writerId,
                nextCheckpointId,
                restoredCheckpointId,
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
        numWrittenElements++;
        elementsWrittenThisCheckpoint.add(element);

        if (writtenElements.contains(element)) {
            LOG.info(
                String.format(
                    "Scott > DeltaSinkWriter[%s] > write :: DUPLICATE ELEMENT element=%s, neckCheckpointId=%s, numWrittenElements=%s",
                    writerId,
                    element,
                    nextCheckpointId,
                    numWrittenElements
                )
            );
        } else {
            writtenElements.add(element);
        }

        // checkpointId is the checkpoint id that this data WILL be checkpointed into
//        if (lastSnapshottedCheckpointId != -1 && nextCheckpointId != lastSnapshottedCheckpointId + 1) {
//            LOG.info(
//                String.format(
//                    "Scott > DeltaSinkWriter[%s] > write :: weird state where nextCheckpointId=%s but lastSnapshottedCheckpointId=%s",
//                    writerId,
//                    nextCheckpointId,
//                    lastSnapshottedCheckpointId
//                )
//            );
//        }

//        if (failCount < 1 && nextCheckpointId > 2 && java.util.concurrent.ThreadLocalRandom.current().nextInt() % 200 == 0) {
//            failCount++;
//
//            int totalRowsBuffered = writerTasksByPartition.values().stream()
//                .mapToInt(DeltaSinkWriterTask::getBufferSize)
//                .sum();
//            final String msg = String.format("RANDOM FAILURE ---- Scott > DeltaSinkWriter[%s] > write :: element=%s :: nextCheckpointId=%s, lastSnapshottedCheckpointId=%s, totalRowsBuffered=%s", writerId, element, nextCheckpointId, lastSnapshottedCheckpointId, totalRowsBuffered);
//            LOG.info(msg);
//            throw new RuntimeException("!!!!!" + msg);
//        }

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

        LOG.info(
            String.format("Scott > DeltaSinkWriter[%s] > write :: element=%s, numWrittenElements=%s, nectCheckpointId=%s", writerId, element, numWrittenElements, nextCheckpointId)
        );
    }

    /**
     * Preparing the commit is the first part of a two-phase commit protocol.
     *
     * Returns the data to commit as the second step of the two-phase commit protocol.
     *
     * This gets called before every checkpoint, even if no data was written
     *
     * All data will be written into the current value of nextCheckpointId. This method then increments nextCheckpointId by 1.
     */
    @Override
    public Collection<DeltaCommittable> prepareCommit() throws IOException, InterruptedException {
        int totalRowsBuffered = writerTasksByPartition.values().stream()
            .mapToInt(DeltaSinkWriterTask::getBufferSize)
            .sum();
        LOG.info(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: nextCheckpointId=%s, totalRowsBuffered=%s", writerId, nextCheckpointId, totalRowsBuffered));

        final Collection<DeltaCommittable> output = new ArrayList<>();

        for (DeltaSinkWriterTask writerTask : writerTasksByPartition.values()) {
            output.addAll(writerTask.prepareCommit());
        }

//        if (delayCount < 1 && java.util.concurrent.ThreadLocalRandom.current().nextInt() % 4 == 0) {
//            delayCount++;
//            LOG.info(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: INTRODUCING DELAY -- START. thisCheckpointId=%s", writerId, nextCheckpointId));
//            try {
//                Thread.sleep(10000);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//            LOG.info(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: INTRODUCING DELAY -- END. thisCheckpointId=%s", writerId, nextCheckpointId));
//        }

        LOG.info(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: elementsWrittenThisCheckpoint=%s", writerId, elementsWrittenThisCheckpoint));

        writerTasksByPartition.clear();
        elementsWrittenThisCheckpoint.clear();

        nextCheckpointId++;

        LOG.info(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: incrementing checkpointId to %s", writerId, nextCheckpointId));

        return output;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        LOG.info(String.format("Scott > DeltaSinkWriter[%s] > flush", writerId));
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

//        if (failCount < 1 && latestCheckpointId >= 2 && java.util.concurrent.ThreadLocalRandom.current().nextInt() % 3 == 0) {
//            failCount++;
//
//            final String msg = String.format("RANDOM FAILURE ---- Scott > DeltaSinkWriter[%s] > snapshotState :: latestCheckpointIdToSnapshot=%s", writerId, latestCheckpointId);
//            LOG.error(msg);
//            throw new RuntimeException("!!!!!" + msg);
//        }

        LOG.info(String.format("Scott > DeltaSinkWriter[%s] > snapshotState :: checkpointIdToSnapshot=%s, nextCheckpointId=%s, ---- %s", writerId, latestCheckpointId, nextCheckpointId, this));

        // WE DONT NEED THIS INFO AT ALL
        // nextCheckpointId + 1 because:
        // we are checkpointing version N
        // nextCheckpointId is N + 1 (all future writes will be a part of checkpoint N + 1)
        // if, before checkpoint N + 1 is complete, there is a failure, then the pipeline will
        // restart, and the checkpointId of all future writes will be N + 2 (which is nextCheckpointId + 1)
        // TODO: probably better to just save the latestCompleteCheckpointId?
        return Collections.singletonList(new DeltaSinkWriterState(appId, writerId, nextCheckpointId + 1));
    }

    @Override
    public String toString() {
        return "DeltaSinkWriter{" +
            "nextCheckpointId=" + nextCheckpointId +
            ", appId='" + appId + '\'' +
            ", engine=" + engine +
            ", writerId='" + writerId + '\'' +
            ", numWriterTasks='" + writerTasksByPartition.size() + '\'' +
            '}';
    }
}
