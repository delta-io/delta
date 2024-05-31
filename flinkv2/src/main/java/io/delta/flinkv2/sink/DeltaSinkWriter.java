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
import java.util.stream.Collectors;

// TODO: implement stateful sink writer
public class DeltaSinkWriter implements CommittingSinkWriter<RowData, Row>{

    private final Engine engine;
    private final String writerId;
    private final Row mockTxnState;
    private final RowType writeOperatorFlinkSchema;
    private final StructType writeOperatorDeltaSchema;
    private Set<String> tablePartitionColumns; // non-final since we set it in try-catch block
    private List<RowData> buffer; // TODO: better data type? store in columnar format?

    private Map<Map<String, Literal>, DeltaSinkWriterTask> writerTasksByPartition;

    public DeltaSinkWriter(String tablePath, RowType writeOperatorFlinkSchema, List<String> userProvidedPartitionColumns) {
        this.engine = DefaultEngine.create(new Configuration());
        this.writerId = java.util.UUID.randomUUID().toString();
        this.writeOperatorFlinkSchema = writeOperatorFlinkSchema;
        this.writeOperatorDeltaSchema = SchemaUtils.toDeltaDataType(writeOperatorFlinkSchema);
        this.buffer = new ArrayList<>();

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
        final Map<String, Literal> partitionValues = DataUtils.flinkRowToPartitionValues(writeOperatorFlinkSchema, element, tablePartitionColumns);

        this.buffer.add(element);

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
        if (buffer.isEmpty()) return Collections.emptyList();

        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: creating logicalData", writerId));
        final CloseableIterator<FilteredColumnarBatch> logicalData =
            flinkRowDataToKernelColumnarBatchClosableIterator();

        final Map<String, Literal> partitionValues =
            DataUtils.flinkRowToPartitionValues(writeOperatorFlinkSchema, buffer.get(0), tablePartitionColumns);

        validatePartitionValues(partitionValues);

        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: partitionValues=%s, buffer.get(0)=%s", writerId, partitionValues, buffer.get(0)));

        this.buffer = new ArrayList<>(); // flush the buffer

        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: creating physicalData", writerId));
        final CloseableIterator<FilteredColumnarBatch> physicalData =
            Transaction.transformLogicalData(engine, mockTxnState, logicalData, partitionValues);

        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: creating writeContext", writerId));
        final DataWriteContext writeContext = Transaction.getWriteContext(engine, mockTxnState, partitionValues);

        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: creating dataFiles", writerId));
        final CloseableIterator<DataFileStatus> dataFiles =
            engine.getParquetHandler()
                .writeParquetFiles(
                    writeContext.getTargetDirectory(),
                    physicalData,
                    writeContext.getStatisticsColumns());

        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: creating partitionDataActions", writerId));
        final CloseableIterator<Row> partitionDataActions =
            Transaction.generateAppendActions(
                engine,
                mockTxnState,
                dataFiles,
                writeContext);

        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: creating output", writerId));
        final Collection<Row> output = new ArrayList<>();
        while (partitionDataActions.hasNext()) {
            output.add(partitionDataActions.next());
        }

        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: output is:\n%s", writerId, output.stream().map(row -> JsonUtils.rowToJson(row)).collect(Collectors.joining("\n"))));
        return output;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > flush", writerId));
    }

    @Override
    public void close() throws Exception {

    }

    ///////////////////
    // Internal APIs //
    ///////////////////

    private CloseableIterator<FilteredColumnarBatch> flinkRowDataToKernelColumnarBatchClosableIterator() {
        final int numColumns = writeOperatorDeltaSchema.length();
        final int size = buffer.size();

        final ColumnVector[] columnVectors = new ColumnVector[numColumns];

        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            final DataType colDataType = writeOperatorDeltaSchema.at(colIdx).getDataType();

            if (colDataType.equivalent(IntegerType.INTEGER)) {
                columnVectors[colIdx] = new IntVectorWrapper(buffer, colIdx);
            }
        }

        return new CloseableIterator<FilteredColumnarBatch>() {
            private boolean hasReturnedSingleElement = false;

            private final FilteredColumnarBatch filteredColumnarBatch = new FilteredColumnarBatch(
                new DefaultColumnarBatch(size, writeOperatorDeltaSchema, columnVectors),
                Optional.empty() /* selectionVector */
            );

            @Override
            public void close() throws IOException {

            }

            @Override
            public boolean hasNext() {
                return !hasReturnedSingleElement;
            }

            @Override
            public FilteredColumnarBatch next() {
                if (!hasNext()) throw new NoSuchElementException();
                hasReturnedSingleElement = true;
                return filteredColumnarBatch;
            }
        };
    }

    private void validatePartitionValues(Map<String, Literal> expectedPartValues) {
        for (RowData rowData : buffer) {
            Map<String, Literal> actualPartValues = DataUtils.flinkRowToPartitionValues(writeOperatorFlinkSchema, rowData, tablePartitionColumns);

            String msg = String.format(
                "Scott > DeltaSinkWriter[%s] > is writing partition value :: %s",
                writerId,
                actualPartValues
            );
            System.out.println(msg);

//            if (actualPartValues.hashCode() != expectedPartValues.hashCode()) {
//                String msg = String.format(
//                    "Scott > DeltaSinkWriter[%s] > validatePartitionValues :: actual=%s did not equal expected =%s",
//                    writerId,
//                    actualPartValues,
//                    expectedPartValues
//                );
//
//                System.out.println(msg);
//
////                throw new RuntimeException(msg);
//            }
        }
    }
}
