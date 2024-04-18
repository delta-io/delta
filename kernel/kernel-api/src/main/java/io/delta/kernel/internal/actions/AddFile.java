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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.DataFileStatus;

import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import static io.delta.kernel.internal.fs.FileOperations.relativizePath;
import static io.delta.kernel.internal.util.PartitionUtils.serializePartitionMap;

/**
 * Delta log action representing an `AddFile`
 */
public class AddFile {

    /* We conditionally read this field based on the query filter */
    private static final StructField JSON_STATS_FIELD = new StructField(
        "stats",
        StringType.STRING,
        true /* nullable */
    );

    /**
     * Schema of the {@code add} action in the Delta Log without stats. Used for constructing
     * table snapshot to read data from the table.
     */
    public static final StructType SCHEMA_WITHOUT_STATS = new StructType()
        .add("path", StringType.STRING, false /* nullable */)
        .add("partitionValues",
            new MapType(StringType.STRING, StringType.STRING, true),
            false /* nullable*/)
        .add("size", LongType.LONG, false /* nullable*/)
        .add("modificationTime", LongType.LONG, false /* nullable*/)
        .add("dataChange", BooleanType.BOOLEAN, false /* nullable*/)
        .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */);

    public static final StructType SCHEMA_WITH_STATS = SCHEMA_WITHOUT_STATS
        .add(JSON_STATS_FIELD);

    /**
     * Full schema of the {@code add} action in the Delta Log.
     */
    public static final StructType FULL_SCHEMA = SCHEMA_WITHOUT_STATS
            .add("stats", StringType.STRING, true /* nullable */)
            .add(
                    "tags",
                    new MapType(StringType.STRING, StringType.STRING, true),
                    true /* nullable */);
    // There are more fields which are added when row-id tracking and clustering is enabled.
    // When Kernel starts supporting row-ids and clustering, we should add those fields here.

    private static final int PATH_ORDINAL = FULL_SCHEMA.indexOf("path");
    private static final int PARTITION_VALUES_ORDINAL = FULL_SCHEMA.indexOf("partitionValues");
    private static final int SIZE_ORDINAL = FULL_SCHEMA.indexOf("size");
    private static final int MODIFICATION_TIME_ORDINAL = FULL_SCHEMA.indexOf("modificationTime");
    private static final int DATA_CHANGE_ORDINAL = FULL_SCHEMA.indexOf("dataChange");
    private static final int STATS_ORDINAL = FULL_SCHEMA.indexOf("stats");

    /**
     * Utility to generate `AddFile` row from the given {@link DataFileStatus} and partition values.
     */
    public static Row convertDataFileStatus(
            URI tableRoot,
            DataFileStatus dataFileStatus,
            Map<String, Literal> partitionValues,
            boolean dataChange) {
        Path filePath = new Path(dataFileStatus.getPath());
        Map<Integer, Object> valueMap = new HashMap<>();
        valueMap.put(PATH_ORDINAL, relativizePath(filePath, tableRoot).toString());
        valueMap.put(PARTITION_VALUES_ORDINAL, serializePartitionMap(partitionValues));
        valueMap.put(SIZE_ORDINAL, dataFileStatus.getSize());
        valueMap.put(MODIFICATION_TIME_ORDINAL, dataFileStatus.getModificationTime());
        valueMap.put(DATA_CHANGE_ORDINAL, dataChange);
        if (dataFileStatus.getStatistics().isPresent()) {
            valueMap.put(STATS_ORDINAL, dataFileStatus.getStatistics().get().serializeAsJson());
        }
        // any fields not present in the valueMap are considered null
        return new GenericRow(FULL_SCHEMA, valueMap);
    }
}
