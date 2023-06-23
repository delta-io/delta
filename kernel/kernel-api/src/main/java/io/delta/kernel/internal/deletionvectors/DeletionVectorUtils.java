package io.delta.kernel.internal.deletionvectors;

import java.io.IOException;
import java.util.Optional;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.*;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.SelectionColumnVector;
import io.delta.kernel.types.StructField;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Utility methods regarding deletion vectors.
 */
// TODO For now adding methods here, revisit where to put this code later
public class DeletionVectorUtils {

    /**
     * TODO
     */
    public static CloseableIterator<DataReadResult> attachSelectionVectors(
            TableClient tableClient,
            Row scanState,
            CloseableIterator<FileDataReadResult> data
    ) {
        String tablePath = scanState.getString(6);

        return new CloseableIterator<DataReadResult>() {

            RoaringBitmapArray currBitmap = null;
            DeletionVectorDescriptor currDV = null;

            @Override
            public boolean hasNext() {
                return data.hasNext();
            }

            @Override
            public DataReadResult next() {
                FileDataReadResult next = data.next();
                Row scanFileRow = next.getScanFileRow();
                DeletionVectorDescriptor dv = DeletionVectorDescriptor.fromRow(
                        scanFileRow.getStruct(5));
                if (dv == null) {
                    return new DataReadResult(next.getData(), Optional.empty());
                }
                if (dv == currDV) {
                    return attachSelectionVector(next.getData(), currBitmap);
                } else {
                    DeletionVectorStoredBitmap storedBitmap = new DeletionVectorStoredBitmap(
                            dv,
                            Optional.of(tablePath));
                    RoaringBitmapArray bitmap = null;
                    try {
                        bitmap = storedBitmap
                                .load(tableClient.getFileSystemClient());
                    } catch (IOException e) {
                        throw new RuntimeException("Couldn't load dv", e);
                    }
                    currBitmap = bitmap;
                    currDV = dv;
                    return attachSelectionVector(next.getData(), bitmap);
                }
            }

            @Override
            public void close() throws IOException {
                data.close();
            }
        };
    }

    static DataReadResult attachSelectionVector(ColumnarBatch data, RoaringBitmapArray bitmap)
    {
        // we can either have SelectionColumnVector(bitmap, row_index_vector)
        // or materialize into a boolean array
        int rowIndexColIdx = data.getSchema().indexOf(StructField.ROW_INDEX_COLUMN_NAME);
        ColumnVector selectionVector = new SelectionColumnVector(
                bitmap, data.getColumnVector(rowIndexColIdx));
        // TODO: remove row_index column here
        return new DataReadResult(data, Optional.of(selectionVector));
    }
}