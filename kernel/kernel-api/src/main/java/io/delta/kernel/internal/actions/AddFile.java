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
import java.util.stream.IntStream;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;
import static io.delta.kernel.utils.Utils.requireNonNull;

/**
 * Delta log action representing an `AddFile`
 */
public class AddFile {

    public static String getPathFromRow(Row row) {
        return requireNonNull(row, 0, "path").getString(0);
    }

    public static DeletionVectorDescriptor getDeletionVectorDescriptorFromRow(Row row) {
        return DeletionVectorDescriptor.fromRow(
            row.getStruct(COL_NAME_TO_ORDINAL.get("deletionVector")));
    }

    // TODO: there are more optional fields in `AddFile` according to the spec. We will be adding
    // them in read schema as we support the related features.
    public static final StructType SCHEMA = new StructType()
        .add("path", StringType.INSTANCE, false /* nullable */)
        .add("partitionValues",
            new MapType(StringType.INSTANCE, StringType.INSTANCE, true),
            false /* nullable*/)
        .add("size", LongType.INSTANCE, false /* nullable*/)
        .add("modificationTime", LongType.INSTANCE, false /* nullable*/)
        .add("dataChange", BooleanType.INSTANCE, false /* nullable*/)
        .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */);

    private static final Map<String, Integer> COL_NAME_TO_ORDINAL =
        IntStream.range(0, SCHEMA.length())
            .boxed()
            .collect(toMap(i -> SCHEMA.at(i).getName(), i -> i));
}
