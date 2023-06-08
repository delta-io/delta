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

package io.delta.kernel.internal.actions;

import java.util.Map;
import java.util.Optional;

import io.delta.kernel.data.Row;
import io.delta.kernel.fs.Path;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

public class RemoveFile extends FileAction {

    public static RemoveFile fromRow(Row row) {
        if (row == null) return null;

        final String path = row.getString(0);
        final long deletionTimestamp = row.getLong(1);
        final Map<String, String> partitionValues = row.getMap(2);
        final long size = row.getLong(3);
        final boolean dataChange = row.getBoolean(4);

        return new RemoveFile(
                path,
                deletionTimestamp,
                partitionValues,
                size,
                dataChange
        );
    }

    public static final StructType READ_SCHEMA = new StructType()
            .add("path", StringType.INSTANCE)
            .add("deletionTimestamp", LongType.INSTANCE)
            .add("partitionValues", new MapType(StringType.INSTANCE, StringType.INSTANCE, false))
            .add("size", LongType.INSTANCE)
            .add("dataChange", BooleanType.INSTANCE);

    private final long deletionTimestamp;
    private final Map<String, String> partitionValues;
    private final long size;

    public RemoveFile(
            String path,
            long deletionTimestamp,
            Map<String, String> partitionValues,
            long size,
            boolean dataChange) {
        super(path, dataChange);

        this.deletionTimestamp = deletionTimestamp;
        this.partitionValues = partitionValues;
        this.size = size;
    }

    public Optional<String> getDeletionVectorUniqueId() {
        return null;
    }

    @Override
    public RemoveFile copyWithDataChange(boolean dataChange) {
        if (this.dataChange == dataChange) {
            return this;
        }
        return new RemoveFile(
                this.path,
                this.deletionTimestamp,
                this.partitionValues,
                this.size,
                dataChange
        );
    }

    public RemoveFile withAbsolutePath(Path dataPath) {
        Path filePath = new Path(path);
        if (filePath.isAbsolute()) {
            return this;
        }
        Path absPath = new Path(dataPath, filePath);
        return new RemoveFile(
                absPath.toString(),
                this.deletionTimestamp,
                this.partitionValues,
                this.size,
                this.dataChange
        );
    }
}
