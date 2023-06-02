package io.delta.kernel.utils;

import java.io.IOException;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

public class Utils {
    /**
     * Utility method to create a singleton {@link CloseableIterator}.
     *
     * @param elem Element to create iterator with.
     * @param <T>  Element type.
     * @return A {@link CloseableIterator} with just one element.
     */
    public static <T> CloseableIterator<T> singletonCloseableIterator(T elem) {
        return new CloseableIterator<T>() {
            private boolean accessed;

            @Override
            public void close() throws IOException {
                // nothing to close
            }

            @Override
            public boolean hasNext() {
                return !accessed;
            }

            @Override
            public T next() {
                accessed = true;
                return elem;
            }
        };
    }

    /**
     * Utility method to create a singleton string {@link ColumnVector}
     *
     * @param value the string element to create the vector with
     * @return A {@link ColumnVector} with a single element {@code value}
     */
    // TODO: add String to method name or make generic?
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
     * Utility method to get the physical schema from the scan state {@link Row} returned by
     * {@link Scan#getScanState(TableClient)}.
     *
     * @param scanState Scan state {@link Row}
     * @return Physical schema to read from the data files.
     */
    public static StructType getPhysicalSchema(Row scanState) {
        // TODO needs io.delta.kernel.internal.data.ScanStateRow
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Get the {@link FileStatus} from given scan file {@link Row}. The {@link FileStatus} contains
     * file metadata about the scan file.
     *
     * @param scanFileInfo {@link Row} representing one scan file.
     * @return a {@link FileStatus} object created from the given scan file row.
     */
    public static FileStatus getFileStatus(Row scanFileInfo) {
        String path = scanFileInfo.getString(0);
        Long size = scanFileInfo.getLong(2);
        boolean hasDeletionVector = scanFileInfo.isNullAt(5);

        return FileStatus.of(path, size, 0, hasDeletionVector);
    }

    // TODO should this be public? Documenting this means exposing details like partitionValues,
    // dataChange flag, etc
    public static Row getScanFileRow(FileStatus fileStatus) {
        // TODO needs io.delta.kernel.internal.actions.AddFile
        throw new UnsupportedOperationException("not implemented yet");
    }
}
