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
package io.delta.kernel.internal.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.delta.kernel.data.ColumnarBatch;

import io.delta.kernel.internal.actions.AddFile;

/**
 * Expose the array of {@link AddFile}s as a {@link ColumnarBatch}.
 */
public class AddFileColumnarBatch
    extends PojoColumnarBatch
{
    private static final Map<Integer, Function<AddFile, Object>> ordinalToAccessor =
        new HashMap<>();
    private static final Map<Integer, String> ordinalToColName = new HashMap<>();

    static {
        ordinalToAccessor.put(0, (a) -> a.getPath());
        ordinalToAccessor.put(1, (a) -> a.getPartitionValues());
        ordinalToAccessor.put(2, (a) -> a.getSize());
        ordinalToAccessor.put(3, (a) -> a.getModificationTime());
        ordinalToAccessor.put(4, (a) -> a.isDataChange());
        ordinalToAccessor.put(5, (a) -> a.getDeletionVectorAsRow());

        ordinalToColName.put(0, "path");
        ordinalToColName.put(1, "partitionValues");
        ordinalToColName.put(2, "size");
        ordinalToColName.put(3, "modificationTime");
        ordinalToColName.put(4, "dataChange");
        ordinalToColName.put(5, "deletionVector");
    }

    // TODO move this somewhere else? Scan File Row?
    private static final Map<String, Integer> colNameToOrdinal = ordinalToColName
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    public static int getDeletionVectorColOrdinal() {
        return colNameToOrdinal.get("deletionVector");
    }

    public AddFileColumnarBatch(List<AddFile> addFiles)
    {
        super(
            requireNonNull(addFiles, "addFiles is null"),
            AddFile.READ_SCHEMA,
            ordinalToAccessor,
            ordinalToColName);
    }
}
