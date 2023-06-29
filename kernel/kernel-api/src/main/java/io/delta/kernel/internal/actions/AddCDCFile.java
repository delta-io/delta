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

import java.util.Collections;
import java.util.Map;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.fs.Path;

import static io.delta.kernel.utils.Utils.requireNonNull;

/**
 * Delta log action representing an `AddCDCFile`
 */
public class AddCDCFile extends FileAction
{
    public static AddCDCFile fromRow(Row row)
    {
        if (row == null) {
            return null;
        }

        final String path = requireNonNull(row, 0, "path").getString(0);
        final Map<String, String> partitionValues = row.getMap(1);
        final long size = requireNonNull(row, 2, "size").getLong(2);
        final boolean dataChange = requireNonNull(row, 3, "dataChange").getBoolean(3);

        return new AddCDCFile(path, partitionValues, size, dataChange);
    }

    // TODO: there are one or more optional fields to add to the schema
    public static final StructType READ_SCHEMA = new StructType()
        .add("path", StringType.INSTANCE, false /* nullable */)
        .add("partitionValues",
            new MapType(StringType.INSTANCE, StringType.INSTANCE, true),
            false /* nullable*/)
        .add("size", LongType.INSTANCE, false /* nullable*/)
        .add("dataChange", BooleanType.INSTANCE, false /* nullable*/);

    private final Map<String, String> partitionValues;
    private final long size;

    public AddCDCFile(
        String path,
        Map<String, String> partitionValues,
        long size,
        boolean dataChange)
    {
        super(path, dataChange);
        this.partitionValues = partitionValues == null ? Collections.emptyMap() : partitionValues;
        this.size = size;
    }

    @Override
    public AddCDCFile copyWithDataChange(boolean dataChange)
    {
        if (this.dataChange == dataChange) {
            return this;
        }
        return new AddCDCFile(
            this.path,
            this.partitionValues,
            this.size,
            dataChange
        );
    }

    public AddCDCFile withAbsolutePath(Path dataPath)
    {
        Path filePath = new Path(path);
        if (filePath.isAbsolute()) {
            return this;
        }
        Path absPath = new Path(dataPath, filePath);
        return new AddCDCFile(
            absPath.toString(),
            this.partitionValues,
            this.size,
            this.dataChange
        );
    }

    public Map<String, String> getPartitionValues()
    {
        return Collections.unmodifiableMap(partitionValues);
    }

    public long getSize()
    {
        return size;
    }

    @Override
    public String toString()
    {
        return "AddCDCFile{" +
            "path='" + path + '\'' +
            ", partitionValues=" + partitionValues +
            ", size=" + size +
            ", dataChange=" + dataChange +
            '}';
    }
}
