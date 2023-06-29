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
import java.util.Optional;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.Utils;

import static io.delta.kernel.utils.Utils.requireNonNull;

/**
 * Delta log action representing an `AddFile`
 */
public class AddFile extends FileAction
{
    public static AddFile fromRow(Row row)
    {
        if (row == null) {
            return null;
        }

        final String path = requireNonNull(row, 0, "path").getString(0);
        final Map<String, String> partitionValues = row.getMap(1);
        final long size = requireNonNull(row, 2, "size").getLong(2);
        final long modificationTime = requireNonNull(row, 3, "modificationTime").getLong(3);
        final boolean dataChange = requireNonNull(row, 4, "dataChange").getBoolean(4);
        final DeletionVectorDescriptor deletionVector =
            DeletionVectorDescriptor.fromRow(row.getStruct(5));

        return new AddFile(
            path, partitionValues, size, modificationTime, dataChange, deletionVector);
    }

    // TODO: there are more optional fields in `AddFile` according to the spec. We will be adding
    // them in read schema as we support the related features.
    public static final StructType READ_SCHEMA = new StructType()
        .add("path", StringType.INSTANCE, false /* nullable */)
        .add("partitionValues",
            new MapType(StringType.INSTANCE, StringType.INSTANCE, true),
            false /* nullable*/)
        .add("size", LongType.INSTANCE, false /* nullable*/)
        .add("modificationTime", LongType.INSTANCE, false /* nullable*/)
        .add("dataChange", BooleanType.INSTANCE, false /* nullable*/)
        .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */);

    private final Map<String, String> partitionValues;
    private final long size;
    private final long modificationTime;
    private final DeletionVectorDescriptor deletionVector;

    public AddFile(
        String path,
        Map<String, String> partitionValues,
        long size,
        long modificationTime,
        boolean dataChange,
        DeletionVectorDescriptor deletionVector)
    {

        super(path, dataChange);
        this.partitionValues = partitionValues == null ? Collections.emptyMap() : partitionValues;
        this.size = size;
        this.modificationTime = modificationTime;
        this.deletionVector = deletionVector;
    }

    @Override
    public AddFile copyWithDataChange(boolean dataChange)
    {
        if (this.dataChange == dataChange) {
            return this;
        }
        return new AddFile(
            this.path,
            this.partitionValues,
            this.size,
            this.modificationTime,
            dataChange,
            this.deletionVector
        );
    }

    public AddFile withAbsolutePath(Path dataPath)
    {
        Path filePath = new Path(path);
        if (filePath.isAbsolute()) {
            return this;
        }
        Path absPath = new Path(dataPath, filePath);
        return new AddFile(
            absPath.toString(),
            this.partitionValues,
            this.size,
            this.modificationTime,
            this.dataChange,
            this.deletionVector
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

    public long getModificationTime()
    {
        return modificationTime;
    }

    public DeletionVectorDescriptor getDeletionVector() {return deletionVector;}

    public Optional<String> getDeletionVectorUniqueId()
    {
        return Optional.ofNullable(deletionVector).map(dv -> dv.getUniqueId());
    }

    public Row getDeletionVectorAsRow()
    {
        if (deletionVector == null) {
            return null;
        }
        else {
            return deletionVector.toRow();
        }
    }

    @Override
    public String toString()
    {
        return "AddFile{" +
            "path='" + path + '\'' +
            ", partitionValues=" + partitionValues +
            ", size=" + size +
            ", modificationTime=" + modificationTime +
            ", dataChange=" + dataChange +
            ", deletionVector=" + deletionVector +
            '}';
    }
}
