package io.delta.kernel.internal.deletionvectors;

import java.io.IOException;
import java.util.Optional;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.*;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.AddFileColumnarBatch;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.data.SelectionColumnVector;
import io.delta.kernel.types.StructField;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Utility methods regarding deletion vectors.
 */
public class DeletionVectorUtils {

    /**
     * For each FileDataReadResult attach its selection vector if its corresponding scan file has
     * a deletion vector.
     */
    public static CloseableIterator<DataReadResult> attachSelectionVectors(
            TableClient tableClient,
            Row scanState,
            CloseableIterator<FileDataReadResult> data)
    {
        String tablePath = ScanStateRow.getTablePath(scanState);

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
                        scanFileRow.getStruct(AddFileColumnarBatch.getDeletionVectorColOrdinal()));

                if (dv == null) { // No deletion vector
                    // TODO: remove row_index column
                    return new DataReadResult(next.getData(), Optional.empty());
                }

                if (!dv.equals(currDV)) {
                    loadNewDvAndBitmap(dv);
                }
                return withSelectionVector(next.getData(), currBitmap);
            }

            private void loadNewDvAndBitmap(DeletionVectorDescriptor dv) {
                DeletionVectorStoredBitmap storedBitmap = new DeletionVectorStoredBitmap(
                        dv,
                        Optional.of(tablePath));
                try {
                    RoaringBitmapArray bitmap = storedBitmap
                            .load(tableClient.getFileSystemClient());
                    currBitmap = bitmap;
                    currDV = dv;
                } catch (IOException e) {
                    throw new RuntimeException("Couldn't load dv", e);
                }
            }

            @Override
            public void close() throws IOException {
                data.close();
            }
        };
    }

    static DataReadResult withSelectionVector(ColumnarBatch data, RoaringBitmapArray bitmap)
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
