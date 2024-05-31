package io.delta.flinkv2.sink;

import io.delta.flinkv2.data.vector.IntVectorWrapper;
import io.delta.flinkv2.utils.DataUtils;
import io.delta.flinkv2.utils.SchemaUtils;
import io.delta.kernel.*;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.*;

// TODO: implement stateful sink writer
public class DeltaSinkWriter implements CommittingSinkWriter<RowData, Row> {

    static final Map<Map<String, Literal>, String> partitionToSinkWriterId = new HashMap<>();

    private final Engine engine;
    private final String writerId;
    private final Row mockTxnState;
    private final RowType writeOperatorFlinkSchema;
    private final StructType writeOperatorDeltaSchema;
    private Set<String> tablePartitionColumns; // non-final since we set it in try-catch block

    private Map<Map<String, Literal>, DeltaSinkWriterTask> writerTasksByPartition;

    public DeltaSinkWriter(String tablePath, RowType writeOperatorFlinkSchema, List<String> userProvidedPartitionColumns) {
        this.engine = DefaultEngine.create(new Configuration());
        this.writerId = java.util.UUID.randomUUID().toString();
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
                "Scott > DeltaSinkWriter > constructor :: writerId=%s, writeOperatorDeltaSchema=%s, txnStateSchema=%s",
                writerId,
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
        final Map<String, Literal> partitionValues =
            DataUtils.flinkRowToPartitionValues(writeOperatorFlinkSchema, element, tablePartitionColumns);

        if (!partitionToSinkWriterId.containsKey(partitionValues)) {
            partitionToSinkWriterId.put(partitionValues, writerId);
            System.out.println(String.format("partitionToSinkWriterId=%s", partitionToSinkWriterId));
        } else if (!partitionToSinkWriterId.get(partitionValues).equalsIgnoreCase(writerId)) {
            throw new RuntimeException(
                String.format(
                    "Partition Value %s was assigned to more than one writer, %s and %s",
                    partitionValues,
                    writerId,
                    partitionToSinkWriterId.get(partitionValues)
                )
            );
        }

        if (!writerTasksByPartition.containsKey(partitionValues)) {
            writerTasksByPartition.put(
                partitionValues,
                new DeltaSinkWriterTask(
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
     */
    @Override
    public Collection<Row> prepareCommit() throws IOException, InterruptedException {
        final Collection<Row> output = new ArrayList<>();

        for (DeltaSinkWriterTask writerTask : writerTasksByPartition.values()) {
            output.addAll(writerTask.prepareCommit());
        }

        return output;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > flush", writerId));
    }

    @Override
    public void close() throws Exception {

    }
}
