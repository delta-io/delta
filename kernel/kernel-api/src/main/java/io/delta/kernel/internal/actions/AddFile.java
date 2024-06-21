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
import java.util.stream.IntStream;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.DataFileStatus;

import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import static io.delta.kernel.internal.util.InternalUtils.relativizePath;
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
        .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */)
        .add("tags",
                new MapType(StringType.STRING, StringType.STRING, true /* valueContainsNull */),
                true /* nullable */);

    public static final StructType SCHEMA_WITH_STATS = SCHEMA_WITHOUT_STATS
        .add(JSON_STATS_FIELD);

    /**
     * Full schema of the {@code add} action in the Delta Log.
     */
    public static final StructType FULL_SCHEMA = SCHEMA_WITH_STATS;
    // There are more fields which are added when row-id tracking and clustering is enabled.
    // When Kernel starts supporting row-ids and clustering, we should add those fields here.

    private static final Map<String, Integer> COL_NAME_TO_ORDINAL =
            IntStream.range(0, FULL_SCHEMA.length())
                    .boxed()
                    .collect(toMap(i -> FULL_SCHEMA.at(i).getName(), i -> i));

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
        valueMap.put(COL_NAME_TO_ORDINAL.get("path"),
                relativizePath(filePath, tableRoot).toUri().toString());
        valueMap.put(COL_NAME_TO_ORDINAL.get("partitionValues"),
                serializePartitionMap(partitionValues));
        valueMap.put(COL_NAME_TO_ORDINAL.get("size"), dataFileStatus.getSize());
        valueMap.put(COL_NAME_TO_ORDINAL.get("modificationTime"),
                dataFileStatus.getModificationTime());
        valueMap.put(COL_NAME_TO_ORDINAL.get("dataChange"), dataChange);
        if (dataFileStatus.getStatistics().isPresent()) {
            valueMap.put(COL_NAME_TO_ORDINAL.get("stats"),
                    dataFileStatus.getStatistics().get().serializeAsJson());
        }
        // any fields not present in the valueMap are considered null
        return new GenericRow(FULL_SCHEMA, valueMap);
    }
}
