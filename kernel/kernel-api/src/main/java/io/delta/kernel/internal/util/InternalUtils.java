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
package io.delta.kernel.internal.util;

import java.io.IOException;
import java.net.URI;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.fs.Path;

public class InternalUtils {
    private static final LocalDate EPOCH_DAY = LocalDate.ofEpochDay(0);
    private static final LocalDateTime EPOCH_DATETIME =
        LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);

    private InternalUtils() {}

    /**
     * Utility method to read at most one row from the given data {@link ColumnarBatch}
     * iterator. If there is more than one row, an exception will be thrown.
     *
     * @param dataIter
     * @return
     */
    public static Optional<Row> getSingularRow(CloseableIterator<ColumnarBatch> dataIter)
        throws IOException {
        Row row = null;
        while (dataIter.hasNext()) {
            try (CloseableIterator<Row> rows = dataIter.next().getRows()) {
                while (rows.hasNext()) {
                    if (row != null) {
                        throw new IllegalArgumentException(
                            "Given data batch contains more than one row");
                    }
                    row = rows.next();
                }
            }
        }
        return Optional.ofNullable(row);
    }

    /**
     * Utility method to read at most one element from a {@link CloseableIterator}.
     * If there is more than element row, an exception will be thrown.
     */
    public static <T> Optional<T> getSingularElement(CloseableIterator<T> iter)
        throws IOException {
        try {
            T result = null;
            while (iter.hasNext()) {
                if (result != null) {
                    throw new IllegalArgumentException(
                        "Iterator contains more than one element");
                }
                result = iter.next();
            }
            return Optional.ofNullable(result);
        } finally {
            iter.close();
        }
    }

    /**
     * Utility method to get the number of days since epoch this given date is.
     */
    public static int daysSinceEpoch(Date date) {
        LocalDate localDate = date.toLocalDate();
        return (int) localDate.toEpochDay();
    }

    /**
     * Utility method to get the number of microseconds since the unix epoch for the given timestamp
     * interpreted in UTC.
     */
    public static long microsSinceEpoch(Timestamp timestamp) {
        LocalDateTime localTimestamp = timestamp.toLocalDateTime();
        return ChronoUnit.MICROS.between(EPOCH_DATETIME, localTimestamp);
    }

    /**
     * Utility method to create a singleton string {@link ColumnVector}
     *
     * @param value the string element to create the vector with
     * @return A {@link ColumnVector} with a single element {@code value}
     */
    public static ColumnVector singletonStringColumnVector(String value) {
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return StringType.STRING;
            }

            @Override
            public int getSize() {
                return 1;
            }

            @Override
            public void close() {
            }

            @Override
            public boolean isNullAt(int rowId) {
                return value == null;
            }

            @Override
            public String getString(int rowId) {
                if (rowId != 0) {
                    throw new IllegalArgumentException("Invalid row id: " + rowId);
                }
                return value;
            }
        };
    }

    public static Row requireNonNull(Row row, int ordinal, String columnName) {
        if (row.isNullAt(ordinal)) {
            throw new IllegalArgumentException(
                "Expected a non-null value for column: " + columnName);
        }
        return row;
    }

    public static ColumnVector requireNonNull(ColumnVector vector, int rowId, String columnName) {
        if (vector.isNullAt(rowId)) {
            throw new IllegalArgumentException(
                "Expected a non-null value for column: " + columnName);
        }
        return vector;
    }

    /**
     * Relativize the given child path with respect to the given root URI. If the child path is
     * already a relative path, it is returned as is.
     *
     * @param child
     * @param root Root directory as URI. Relativization is done with respect to this root.
     *             The relativize operation requires conversion to URI, so the caller is expected to
     *             convert the root directory to URI once and use it for relativizing for multiple
     *             child paths.
     * @return
     */
    public static Path relativizePath(Path child, URI root) {
        if (child.isAbsolute()) {
            return new Path(root.relativize(child.toUri()));
        }
        return child;
    }

    public static Set<String> toLowerCaseSet(Collection<String> set) {
        return set.stream().map(String::toLowerCase).collect(Collectors.toSet());
    }
}
