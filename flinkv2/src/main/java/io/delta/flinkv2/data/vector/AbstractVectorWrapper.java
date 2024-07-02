package io.delta.flinkv2.data.vector;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.DataType;
import org.apache.flink.table.data.RowData;

import java.util.List;

public class AbstractVectorWrapper implements ColumnVector {
    private final DataType dataType;

    protected final List<RowData> bufferReference;
    protected final int colIdx;

    public AbstractVectorWrapper(
            DataType dataType,
            List<RowData> bufferReference,
            int colIdx) {
        this.dataType = dataType;
        this.bufferReference = bufferReference;
        this.colIdx = colIdx;
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public int getSize() {
        return bufferReference.size();
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isNullAt(int rowId) {
        checkValidRowId(rowId);
        return bufferReference.get(rowId).isNullAt(colIdx);
    }

    protected void checkValidRowId(int rowId) {
        if (rowId < 0 || rowId >= getSize()) {
            throw new IllegalArgumentException("invalid row access: " + rowId);
        }
    }
}
