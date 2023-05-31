package io.delta.kernel.utils;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.data.AddFileColumnarBatch;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Utils
{
    /**
     * Utility method to create a singleton {@link CloseableIterator}.
     * @param elem Element to create iterator with.
     * @param <T> Element type.
     * @return A {@link CloseableIterator} with just one element.
     */
    public static <T> CloseableIterator<T> singletonCloseableIterator(T elem)  {
        return new CloseableIterator<T>() {
            private boolean accessed;
            @Override
            public void close()
                    throws IOException
            {
                // nothing to close
            }

            @Override
            public boolean hasNext()
            {
                return !accessed;
            }

            @Override
            public T next()
            {
                accessed = true;
                return elem;
            }
        };
    }

    public static ColumnVector singletonColumnVector(String value) {
        return new ColumnVector() {
            @Override
            public DataType getDataType()
            {
                return StringType.INSTANCE;
            }

            @Override
            public int getSize()
            {
                return 1;
            }

            @Override
            public void close() {}

            @Override
            public boolean isNullAt(int rowId)
            {
                return value == null;
            }

            @Override
            public String getString(int rowId)
            {
                if (rowId != 0) {
                    throw new IllegalArgumentException("Invalid row id: " + rowId);
                }
                return value;
            }
        };
    }

    /**
     * Utility method to get the physical schema from the scan state {@link Row}.
     * @param scanState Scan state given as {@link Row}
     * @return Physical schema to read from the data files.
     */
    public static StructType getPhysicalSchema(Row scanState) {
        // TODO: pending serialization and deserialization of the `StructType` schmea
        return ((ScanStateRow) scanState).getReadSchema();
    }

    /**
     * Get the {@link FileStatus} from given scan file row. {@link FileStatus} object allows the
     * connector to look at the partial metadata of the scan file.
     *
     * @param scanFileInfo Row representing one scan file.
     * @return a {@link FileStatus} object created from the given scan file row.
     */
    public static FileStatus getFileStatus(Row scanFileInfo) {
        String path = scanFileInfo.getString(0);
        Long size = scanFileInfo.getLong(2);
        boolean hasDeletionVector = scanFileInfo.isNullAt(5);

        return FileStatus.of(path, size, 0, hasDeletionVector);
    }

    public static Row getScanFileRow(FileStatus fileStatus) {
        AddFile addFile = new AddFile(
                fileStatus.getPath(),
                Collections.emptyMap(),
                fileStatus.getSize(),
                fileStatus.getModificationTime(),
                false /* dataChange */,
                "" /* deletionVector */
        );

        return new AddFileColumnarBatch(Collections.singletonList(addFile))
                .getRows()
                .next();
    }
}
