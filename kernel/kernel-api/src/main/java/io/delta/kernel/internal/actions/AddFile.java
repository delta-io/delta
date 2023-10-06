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

import io.delta.kernel.types.*;

/**
 * Delta log action representing an `AddFile`
 */
public class AddFile {
    // Note: these are more optional fields in `AddFile` according to the spec. We will be adding
    // them in read schema as we support the related features.
    public static final StructType SCHEMA = new StructType()
        .add("path", StringType.STRING, false /* nullable */)
        .add("partitionValues",
            new MapType(StringType.STRING, StringType.STRING, true),
            false /* nullable*/)
        .add("size", LongType.LONG, false /* nullable*/)
        .add("modificationTime", LongType.LONG, false /* nullable*/)
        .add("dataChange", BooleanType.BOOLEAN, false /* nullable*/)
        .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */);
}
