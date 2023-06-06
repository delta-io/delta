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

/**
 * Interface encapsulating all clients needed by the Delta Kernel in order to read the
 * Delta table. Connectors are expected to pass an implementation of this interface when reading
 * a Delta table.
 */
public interface TableClient
{

    /**
     * Get the connector provided {@link ExpressionHandler}.
     * @return An implementation of {@link ExpressionHandler}.
     */
    ExpressionHandler getExpressionHandler();

    /**
     * Get the connector provided {@link JsonHandler}.
     * @return An implementation of {@link JsonHandler}.
     */
    JsonHandler getJsonHandler();

    /**
     * Get the connector provided {@link FileSystemClient}.
     * @return An implementation of {@link FileSystemClient}.
     */
    FileSystemClient getFileSystemClient();

    /**
     * Get the connector provided {@link ParquetHandler}.
     * @return An implementation of {@link ParquetHandler}.
     */
    ParquetHandler getParquetHandler();
}
