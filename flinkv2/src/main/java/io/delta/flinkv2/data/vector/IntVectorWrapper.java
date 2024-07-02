package io.delta.flinkv2.data.vector;

import io.delta.kernel.types.IntegerType;
import org.apache.flink.table.data.RowData;

import java.util.List;

public class IntVectorWrapper extends AbstractVectorWrapper {

    public IntVectorWrapper(List<RowData> bufferReference, int colIdx) {
        super(IntegerType.INTEGER, bufferReference, colIdx);
    }

    @Override
    public int getInt(int rowId) {
        checkValidRowId(rowId);
        return bufferReference.get(rowId).getInt(colIdx);
    }
}
