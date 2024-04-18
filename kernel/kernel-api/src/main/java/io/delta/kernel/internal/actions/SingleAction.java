/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import io.delta.kernel.types.StructType;

public class SingleAction {
    /**
     * Get the schema of reading entries from Delta Log delta and checkpoint files for construction
     * of new checkpoint.
     */
    public static StructType CHECKPOINT_SCHEMA = new StructType()
            .add("txn", SetTransaction.READ_SCHEMA)
            .add("add", AddFile.FULL_SCHEMA)
            .add("remove", RemoveFile.FULL_SCHEMA)
            .add("metaData", Metadata.READ_SCHEMA)
            .add("protocol", Protocol.READ_SCHEMA);
    // Once we start supporting updating CDC or domain metadata enabled tables, we should add the
    // schema for those fields here.
}
