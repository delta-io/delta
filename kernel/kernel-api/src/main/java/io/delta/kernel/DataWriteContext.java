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
package io.delta.kernel;

import java.util.List;
import java.util.Map;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;

/**
 * Contains the context for writing data to Delta table. The context is created for each partition
 * for partitioned table or once per table for un-partitioned table. It is created using
 * {@link Transaction#getWriteContext(Engine, Row, Map)} (String, Map, List)}.
 *
 * @since 3.2.0
 */
@Evolving
public interface DataWriteContext {
    /**
     * Returns the target directory where the data should be written.
     *
     * @return fully qualified path of the target directory
     */
    String getTargetDirectory();

    /**
     * Returns the list of {@link Column} that the connector can optionally collect statistics. Each
     * {@link Column} is a reference to a top-level or nested column in the table.
     * <p>
     * Statistics collections can be skipped or collected for a partial list of the returned
     * {@link Column}s. When stats are present in the written Delta log, they can be used to
     * optimize query performance.
     *
     * @return schema of the statistics
     */
    List<Column> getStatisticsColumns();
}
