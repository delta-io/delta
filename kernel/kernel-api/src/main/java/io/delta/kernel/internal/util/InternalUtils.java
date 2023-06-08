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
package io.delta.kernel.internal.util;

import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.data.AddFileColumnarBatch;

import java.util.Collections;

public class InternalUtils
{
    private InternalUtils() {}

    public static Row getScanFileRow(FileStatus fileStatus) {
        AddFile addFile = new AddFile(
                fileStatus.getPath(),
                Collections.emptyMap(),
                fileStatus.getSize(),
                fileStatus.getModificationTime(),
                false /* dataChange */
        );

        return new AddFileColumnarBatch(Collections.singletonList(addFile))
                .getRows()
                .next();
    }
}
