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
package io.delta.kernel.internal.replay;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.Tuple2;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

public class LogReplayUtils {

    private LogReplayUtils() {
    }

    public static class UniqueFileActionTuple extends Tuple2<URI, Optional<String>> {
        UniqueFileActionTuple(URI fileURI, Optional<String> deletionVectorId) {
            super(fileURI, deletionVectorId);
        }
    }

    public static UniqueFileActionTuple getUniqueFileAction(
            ColumnVector pathVector,
            ColumnVector dvVector,
            int rowId) {
        final String path = pathVector.getString(rowId);
        final URI pathAsUri = pathToUri(path);
        final Optional<String> dvId = Optional.ofNullable(
                DeletionVectorDescriptor.fromColumnVector(dvVector, rowId)
        ).map(DeletionVectorDescriptor::getUniqueId);

        return new UniqueFileActionTuple(pathAsUri, dvId);
    }

    /**
     * Verifies that a set of delta or checkpoint files to be read actually belongs to this table.
     * Visible only for testing.
     */
    public static void assertLogFilesBelongToTable(Path logPath, List<FileStatus> allFiles) {
        String logPathStr = logPath.toString(); // fully qualified path
        for (FileStatus fileStatus : allFiles) {
            String filePath = fileStatus.getPath();
            if (!filePath.startsWith(logPathStr)) {
                throw new RuntimeException("File (" + filePath + ") doesn't belong in the " +
                        "transaction log at " + logPathStr + ".");
            }
        }
    }

    static boolean[] prepareSelectionVectorBuffer(boolean[] currentSelectionVector, int newSize) {
        if (currentSelectionVector == null || currentSelectionVector.length < newSize) {
            currentSelectionVector = new boolean[newSize];
        } else {
            // reset the array - if we are reusing the same buffer.
            Arrays.fill(currentSelectionVector, false);
        }
        return currentSelectionVector;
    }

    static URI pathToUri(String path) {
        try {
            return new URI(path);
        } catch (URISyntaxException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Get the ordinals of the column path at each level. Ordinal refers position of a column within
     * a struct type column. For example: `struct(a: struct(a1: int, b1: long))` and lookup path is
     * `a.b1` returns `0, 1`.
     */
    static int[] getPathOrdinals(StructType schema, String... path) {
        checkArgument(path.length > 0, "Invalid path");
        int[] pathOrdinals = new int[path.length];
        DataType currentLevelDataType = schema;
        for (int level = 0; level < path.length; level++) {
            checkArgument(currentLevelDataType instanceof StructType, "Invalid search path");
            StructType asStructType = (StructType) currentLevelDataType;
            pathOrdinals[level] = asStructType.indexOf(path[level]);
            currentLevelDataType = asStructType.at(pathOrdinals[level]).getDataType();
        }
        return pathOrdinals;
    }

    /**
     * Get the vector corresponding to the given ordinals at each level of the column path.
     */
    static ColumnVector getVector(ColumnarBatch batch, int[] pathOrdinals) {
        checkArgument(pathOrdinals.length > 0, "Invalid path ordinals size");
        ColumnVector vector = null;
        for (int level = 0; level < pathOrdinals.length; level++) {
            int levelOrdinal = pathOrdinals[level];
            vector = (level == 0) ? batch.getColumnVector(levelOrdinal) :
                    vector.getChild(levelOrdinal);
        }

        return vector;
    }
}
