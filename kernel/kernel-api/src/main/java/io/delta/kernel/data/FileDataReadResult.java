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

package io.delta.kernel.data;


/**
 * Data read from a Delta table file and the corresponding scan file information.
 */
public interface FileDataReadResult
{
    /**
     * Get the data read from the file.
     * @return Data in {@link ColumnarBatch} format.
     */
    ColumnarBatch getData();

    /**
     * Get the scan file information of the file from which the data is read as a {@link Row}. This
     * should be the same {@link Row} that Delta Kernel provided when reading/contextualizing
     * the file.
     * @return a scan file {@link Row}
     */
    Row getScanFileRow();
}
