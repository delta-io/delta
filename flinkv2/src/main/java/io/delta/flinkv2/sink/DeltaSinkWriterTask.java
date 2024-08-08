package io.delta.flinkv2.sink;

import io.delta.flinkv2.data.vector.IntVectorWrapper;
import io.delta.flinkv2.utils.DataUtils;
import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class DeltaSinkWriterTask {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaSinkWriterTask.class);

    private final String appId;
    private final String writerId; // checkpointId that all FUTURE writes will be a part of
    private final long checkpointId;
    private final Engine engine;
    private final String writerTaskId;
    private List<RowData> buffer;
    private final Map<String, Literal> partitionValues;
    private final Row mockTxnState;
    private final StructType writeOperatorDeltaSchema;

    public DeltaSinkWriterTask(
            String appId,
            String writerId,
            long checkpointId,
            Engine engine,
            Map<String, Literal> partitionValues,
            Row mockTxnState,
            StructType writeOperatorDeltaSchema,
            String parentWriterId) {
        this.appId = appId;
        this.writerId = writerId;
        this.checkpointId = checkpointId;
        this.engine = engine;
        this.writerTaskId = java.util.UUID.randomUUID().toString();
        this.buffer = new ArrayList<>();
        this.partitionValues = partitionValues;
        this.mockTxnState = mockTxnState;
        this.writeOperatorDeltaSchema = writeOperatorDeltaSchema;

        LOG.info(
            String.format(
                "Scott > DeltaSinkWriterTask > constructor :: parentWriterId=%s, writerTaskId=%s, partitionValues=%s",
                parentWriterId,
                writerTaskId,
                partitionValues
            )
        );
    }

    public int getBufferSize() {
        return buffer.size();
    }

    public void write(RowData element, SinkWriter.Context context) throws IOException, InterruptedException {
        buffer.add(element);
    }

    public Collection<DeltaCommittable> prepareCommit() throws IOException, InterruptedException {
        if (buffer.isEmpty()) return Collections.emptyList();

        LOG.info(String.format("Scott > DeltaSinkWriterTask[%s] > prepareCommit :: creating logicalData", writerTaskId));
        final CloseableIterator<FilteredColumnarBatch> logicalData =
            flinkRowDataToKernelColumnarBatchClosableIterator();

        LOG.info(String.format("Scott > DeltaSinkWriterTask[%s] > prepareCommit :: partitionValues=%s, buffer.get(0)=%s", writerTaskId, partitionValues, buffer.get(0)));

        this.buffer = new ArrayList<>(); // flush the buffer

        LOG.info(String.format("Scott > DeltaSinkWriterTask[%s] > prepareCommit :: creating physicalData", writerTaskId));
        final CloseableIterator<FilteredColumnarBatch> physicalData =
            Transaction.transformLogicalData(engine, mockTxnState, logicalData, partitionValues);

        LOG.info(String.format("Scott > DeltaSinkWriterTask[%s] > prepareCommit :: creating writeContext", writerTaskId));
        final DataWriteContext writeContext = Transaction.getWriteContext(engine, mockTxnState, partitionValues);

        LOG.info(String.format("Scott > DeltaSinkWriterTask[%s] > prepareCommit :: creating dataFiles", writerTaskId));
        final CloseableIterator<DataFileStatus> dataFiles =
            engine.getParquetHandler()
                .writeParquetFiles(
                    writeContext.getTargetDirectory(),
                    physicalData,
                    writeContext.getStatisticsColumns());

        LOG.info(String.format("Scott > DeltaSinkWriterTask[%s] > prepareCommit :: creating partitionDataActions", writerTaskId));
        final CloseableIterator<Row> partitionDataActions =
            Transaction.generateAppendActions(
                engine,
                mockTxnState,
                dataFiles,
                writeContext);

        LOG.info(String.format("Scott > DeltaSinkWriterTask[%s] > prepareCommit :: creating output", writerTaskId));
        final Collection<DeltaCommittable> output = new ArrayList<>();

        while (partitionDataActions.hasNext()) {
            DeltaCommittable deltaCommittable = new DeltaCommittable(
                appId,
                writerId,
                checkpointId,
                partitionDataActions.next()
            );

            LOG.info(String.format("Scott > DeltaSinkWriterTask[%s] > prepareCommit :: created %s", writerTaskId, deltaCommittable));

            output.add(deltaCommittable);
        }

        LOG.info(String.format("Scott > DeltaSinkWriterTask[%s] > prepareCommit :: returning output", writerTaskId));

        return output;
    }

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
