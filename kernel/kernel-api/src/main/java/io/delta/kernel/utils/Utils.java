/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.kernel.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.delta.kernel.Scan;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.types.TableSchemaSerDe;

public class Utils
{
    /**
     * Utility method to create a singleton {@link CloseableIterator}.
     *
     * @param elem Element to create iterator with.
     * @param <T> Element type.
     * @return A {@link CloseableIterator} with just one element.
     */
    public static <T> CloseableIterator<T> singletonCloseableIterator(T elem)
    {
        return new CloseableIterator<T>()
        {
            private boolean accessed;

            @Override
            public void close() throws IOException
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

    /**
     * Convert a {@link Iterator} to {@link CloseableIterator}. Useful when passing normal iterators
     * for arguments that require {@link CloseableIterator} type.
     *
     * @param iter {@link Iterator} instance
     * @param <T> Element type
     * @return A {@link CloseableIterator} wrapping the given {@link Iterator}
     */
    public static <T> CloseableIterator<T> toCloseableIterator(Iterator<T> iter)
    {
        return new CloseableIterator<T>()
        {
            @Override
            public void close() {}

            @Override
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            @Override
            public T next()
            {
                return iter.next();
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
    public static ColumnVector singletonColumnVector(String value)
    {
        return new ColumnVector()
        {
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
            public void close()
            {
            }

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
     * Utility method to get the logical schema from the scan state {@link Row} returned by
     * {@link Scan#getScanState(TableClient)}.
     *
     * @param tableClient instance of {@link TableClient} to use.
     * @param scanState Scan state {@link Row}
     * @return Logical schema to read from the data files.
     */
    public static StructType getLogicalSchema(TableClient tableClient, Row scanState)
    {
        int schemaStringOrdinal = ScanStateRow.getLogicalSchemaStringColOrdinal();
        String serializedSchema = scanState.getString(schemaStringOrdinal);
        return TableSchemaSerDe.fromJson(tableClient.getJsonHandler(), serializedSchema);
    }

    /**
     * Utility method to get the physical schema from the scan state {@link Row} returned by
     * {@link Scan#getScanState(TableClient)}.
     *
     * @param tableClient instance of {@link TableClient} to use.
     * @param scanState Scan state {@link Row}
     * @return Physical schema to read from the data files.
     */
    public static StructType getPhysicalSchema(TableClient tableClient, Row scanState)
    {
        int schemaStringOrdinal = ScanStateRow.getPhysicalSchemaStringColOrdinal();
        String serializedSchema = scanState.getString(schemaStringOrdinal);
        return TableSchemaSerDe.fromJson(tableClient.getJsonHandler(), serializedSchema);
    }

    /**
     * Get the list of partition column names from the scan state {@link Row} returned by
     * {@link Scan#getScanState(TableClient)}.
     *
     * @param scanState Scan state {@link Row}
     * @return List of partition column names according to the scan state.
     */
    public static List<String> getPartitionColumns(Row scanState)
    {
        int partitionColumnsOrdinal = ScanStateRow.getPartitionColumnsColOrdinal();
        return scanState.getArray(partitionColumnsOrdinal);
    }

    /**
     * Get the column mapping mode from the scan state {@link Row} returned by
     * {@link Scan#getScanState(TableClient)}.
     */
    public static String getColumnMappingMode(Row scanState)
    {
        int configOrdinal = ScanStateRow.getConfigurationColOrdinal();
        Map<String, String> configuration = scanState.getMap(configOrdinal);
        String cmMode = configuration.get("delta.columnMapping.mode");
        return cmMode == null ? "none" : cmMode;
    }

    /**
     * Get the {@link FileStatus} from given scan file {@link Row}. The {@link FileStatus} contains
     * file metadata about the scan file.
     *
     * @param scanFileInfo {@link Row} representing one scan file.
     * @return a {@link FileStatus} object created from the given scan file row.
     */
    public static FileStatus getFileStatus(Row scanFileInfo)
    {
        String path = scanFileInfo.getString(0);
        Long size = scanFileInfo.getLong(2);

        return FileStatus.of(path, size, 0);
    }

    /**
     * Get the partition columns and value belonging to the given scan file row.
     *
     * @param scanFileInfo {@link Row} representing one scan file.
     * @return Map of partition column name to partition column value.
     */
    public static Map<String, String> getPartitionValues(Row scanFileInfo)
    {
        return scanFileInfo.getMap(1);
    }

    /**
     * Close the given one or more {@link Closeable}s. {@link Closeable#close()}
     * will be called on all given non-null closeables. Will throw unchecked
     * {@link RuntimeException} if an error occurs while closing. If multiple closeables causes
     * exceptions in closing, the exceptions will be added as suppressed to the main exception
     * that is thrown.
     *
     * @param closeables
     */
    public static void closeCloseables(Closeable... closeables)
    {
        RuntimeException exception = null;
        for (Closeable closeable : closeables) {
            if (closeable == null) {
                continue;
            }
            try {
                closeable.close();
            }
            catch (Exception ex) {
                if (exception == null) {
                    exception = new RuntimeException(ex);
                }
                else {
                    exception.addSuppressed(ex);
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Close the given list of {@link Closeable} objects. Any exception thrown is silently ignored.
     *
     * @param closeables
     */
    public static void closeCloseablesSilently(Closeable... closeables)
    {
        try {
            closeCloseables(closeables);
        }
        catch (Throwable throwable) {
            // ignore
        }
    }

    public static Row requireNonNull(Row row, int ordinal, String columnName)
    {
        if (row.isNullAt(ordinal)) {
            throw new IllegalArgumentException("Expected a non-null value for column: " + columnName);
        }
        return row;
    }
}
