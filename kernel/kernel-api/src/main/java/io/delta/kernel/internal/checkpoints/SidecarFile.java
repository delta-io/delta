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

import java.util.Objects;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

public class SidecarFile {
    public String path;
    public long sizeInBytes;
    public long modificationTime;

    public static SidecarFile fromRow(Row row) {
        return new SidecarFile(
                row.getString(0),
                row.getLong(1),
                row.getLong(2)
        );
    }

    public SidecarFile(String path, long sizeInBytes, long modificationTime) {
        this.path = path;
        this.sizeInBytes = sizeInBytes;
        this.modificationTime = modificationTime;
    }

    public static StructType READ_SCHEMA = new StructType()
            .add("path", StringType.STRING, false /* nullable */)
            .add("sizeInBytes", LongType.LONG, false /* nullable */)
            .add("modificationTime", LongType.LONG, false /* nullable */);

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        SidecarFile otherSidecarFile = (SidecarFile) other;
        return this.path.equals(otherSidecarFile.path) &&
                this.sizeInBytes == otherSidecarFile.sizeInBytes &&
                this.modificationTime == otherSidecarFile.modificationTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, sizeInBytes, modificationTime);
    }
}
