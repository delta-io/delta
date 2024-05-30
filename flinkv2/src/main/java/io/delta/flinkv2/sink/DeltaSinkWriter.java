package io.delta.flinkv2.sink;

import io.delta.flinkv2.data.vector.MutableAbstractColumnVector;
import io.delta.flinkv2.data.vector.MutableIntColumnVector;
import io.delta.flinkv2.data.vector.MutableLongColumnVector;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.*;

public class DeltaSinkWriter implements CommittingSinkWriter<RowData, DeltaCommittable> {

    private final String writerId;
    private final Row txnState;
    private final List<RowData> buffer; // TODO: better data type? store in columnar format?

    public DeltaSinkWriter(Row txnState) {
        this.writerId = java.util.UUID.randomUUID().toString();
        this.txnState = txnState;
        this.buffer = new ArrayList<>();

        System.out.println(
            String.format(
                "Scott > DeltaSinkWriter > constructor :: writerId=%s, txnStateSchema=%s",
                writerId,
                txnState.getSchema()
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
    public Collection<DeltaCommittable> prepareCommit() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // TODO: perhaps have the writer write (i.e. flush) the data here, as this can be called
        //       multiple times?
    }

    @Override
    public void close() throws Exception {

    }

    ///////////////////
    // Internal APIs //
    ///////////////////

    private void executeWrite() {
        final CloseableIterator<FilteredColumnarBatch> data =
            flinkRowDataToKernelColumnarBatchClosableIterator(buffer, txnState.getSchema());
    }

    private static CloseableIterator<FilteredColumnarBatch> flinkRowDataToKernelColumnarBatchClosableIterator(
            List<RowData> flinkRowData,
            StructType deltaTableSchema) {
        final int numColumns = deltaTableSchema.length();
        final int size = flinkRowData.size();

        final MutableAbstractColumnVector[] mutableColumnVectors =
            new MutableAbstractColumnVector[numColumns];

        // Step 1: init the mutableColumnVectors
        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            final DataType colDataType = deltaTableSchema.at(colIdx).getDataType();

            if (colDataType.equivalent(IntegerType.INTEGER)) {
                mutableColumnVectors[colIdx] = new MutableIntColumnVector(size);
            } else if (colDataType.equivalent(LongType.LONG)) {
                mutableColumnVectors[colIdx] = new MutableLongColumnVector(size);
            }
        }

        // Step 2: populate the mutableColumnVectors with the data
        int rowIdx = 0;
        for (RowData rowData : flinkRowData) {
            for (int colIdx = 0; colIdx < numColumns; colIdx++) {
                final DataType colDataType = deltaTableSchema.at(colIdx).getDataType();

                if (rowData.isNullAt(colIdx)) {
                    mutableColumnVectors[colIdx].setIsNullAt(rowIdx);
                } else if (colDataType.equivalent(IntegerType.INTEGER)) {
                    mutableColumnVectors[colIdx].setInt(rowIdx, rowData.getInt(colIdx));
                } else if (colDataType.equivalent(LongType.LONG)) {
                    mutableColumnVectors[colIdx].setLong(rowIdx, rowData.getLong(colIdx));
                } else {
                    throw new UnsupportedOperationException(
                        String.format("Type not supported: %s", colDataType)
                    );
                }
            }

            rowIdx++;
        }

        return new CloseableIterator<FilteredColumnarBatch>() {
            private boolean hasReturnedSingleElement = false;

            private final FilteredColumnarBatch filteredColumnarBatch = new FilteredColumnarBatch(
                new DefaultColumnarBatch(size, deltaTableSchema, mutableColumnVectors),
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
