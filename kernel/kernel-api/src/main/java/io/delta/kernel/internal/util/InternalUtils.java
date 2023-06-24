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
import java.util.Collections;
import java.util.Optional;

import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.data.AddFileColumnarBatch;

public class InternalUtils
{
    private InternalUtils() {}

    public static Row getScanFileRow(FileStatus fileStatus)
    {
        AddFile addFile = new AddFile(
            fileStatus.getPath(),
            Collections.emptyMap(),
            fileStatus.getSize(),
            fileStatus.getModificationTime(),
            false /* dataChange */,
            null // deletionVector
        );

        return new AddFileColumnarBatch(Collections.singletonList(addFile))
            .getRows()
            .next();
    }

    /**
     * Utility method to read at most one row from the given data {@link FileDataReadResult} iterator.
     * If there is more than one row, an exception will be thrown.
     *
     * @param dataIter
     * @return
     */
    public static Optional<Row> getSingularRow(CloseableIterator<FileDataReadResult> dataIter)
        throws IOException
    {
        Row row = null;
        while (dataIter.hasNext()) {
            try (CloseableIterator<Row> rows = dataIter.next().getData().getRows()) {
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
        throws IOException
    {
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
     * Precondition-style validation that throws {@link IllegalArgumentException}.
     *
     * @param isValid {@code true} if valid, {@code false} if an exception should be thrown
     * @throws IllegalArgumentException if {@code isValid} is false
     */
    public static void checkArgument(boolean isValid)
            throws IllegalArgumentException
    {
        if (!isValid) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Precondition-style validation that throws {@link IllegalArgumentException}.
     *
     * @param isValid {@code true} if valid, {@code false} if an exception should be thrown
     * @param message A String message for the exception.
     * @throws IllegalArgumentException if {@code isValid} is false
     */
    public static void checkArgument(boolean isValid, String message)
            throws IllegalArgumentException
    {
        if (!isValid) {
            throw new IllegalArgumentException(message);
        }
    }
}
