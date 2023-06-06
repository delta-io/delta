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

package io.delta.kernel.client;

import io.delta.kernel.data.Row;

/**
 * Placeholder interface allowing connectors to attach their own custom implementation. Connectors
 * can use this to pass additional context about a scan file through Delta Kernel and back to the
 * connector for interpretation.
 */
public interface FileReadContext
{
    /**
     * Get the scan file info associated with the read context.
     * @return scan file {@link Row}
     */
    Row getScanFileRow();
}
