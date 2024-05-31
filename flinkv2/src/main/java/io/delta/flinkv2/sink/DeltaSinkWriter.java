package io.delta.flinkv2.sink;

import io.delta.flinkv2.data.vector.IntVectorWrapper;
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
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class DeltaSinkWriter implements CommittingSinkWriter<RowData, Row> {

    private final Engine engine;
    private final String writerId;
    private final Row mockTxnState;
    private final StructType writeOperatorDeltaSchema;
    private final List<String> partitionColumns;
    private List<RowData> buffer; // TODO: better data type? store in columnar format?

    public DeltaSinkWriter(String tablePath, RowType writeOperatorFlinkSchema, List<String> partitionColumns) {
        this.engine = DefaultEngine.create(new Configuration());
        this.writerId = java.util.UUID.randomUUID().toString();
        this.writeOperatorDeltaSchema = SchemaUtils.toDeltaDataType(writeOperatorFlinkSchema);
        this.partitionColumns = partitionColumns;
        this.buffer = new ArrayList<>();

        final Table table = Table.forPath(engine, tablePath);
        TransactionBuilder txnBuilder =
            table.createTransactionBuilder(
                engine,
                "FlinkV2",
                Operation.MANUAL_UPDATE // this doesn't matter, we aren't committing anything
            );

        try {
            table.getLatestSnapshot(engine);
        } catch (TableNotFoundException ex) {
            // table doesn't exist
            txnBuilder = txnBuilder
                .withSchema(engine, writeOperatorDeltaSchema)
                .withPartitionColumns(engine, partitionColumns);
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

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
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
        System.out.println(String.format("Scott > DeltaSinkWriter[%s] > prepareCommit :: creating logicalData", writerId));
        final CloseableIterator<FilteredColumnarBatch> logicalData =
            flinkRowDataToKernelColumnarBatchClosableIterator();

        this.buffer = new ArrayList<>(); // flush the buffer

        final Map<String, Literal> partitionValues = new HashMap<>(); // TODO: partition values

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
}
