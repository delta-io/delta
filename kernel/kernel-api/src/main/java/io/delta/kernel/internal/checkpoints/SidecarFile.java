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
package io.delta.kernel.internal.checkpoints;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

/**
 * Action representing a SidecarFile in a top-level V2 checkpoint file.
 */
public class SidecarFile {
    public static StructType READ_SCHEMA = new StructType()
            .add("path", StringType.STRING, false /* nullable */)
            .add("sizeInBytes", LongType.LONG, false /* nullable */)
            .add("modificationTime", LongType.LONG, false /* nullable */);

    public static SidecarFile fromColumnVector(ColumnVector vector, int rowIndex) {
        if (vector.isNullAt(rowIndex)) {
            return null;
        }
        return new SidecarFile(
                vector.getChild(0).getString(rowIndex),
                vector.getChild(1).getLong(rowIndex),
                vector.getChild(2).getLong(rowIndex)
        );
    }

    private final String path;
    private final long sizeInBytes;
    private final long modificationTime;

    public SidecarFile(String path, long sizeInBytes, long modificationTime) {
        this.path = path;
        this.sizeInBytes = sizeInBytes;
        this.modificationTime = modificationTime;
    }

    public String getPath() {
        return path;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public long getModificationTime() {
        return modificationTime;
    }
}
