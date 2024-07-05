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
package io.delta.kernel.internal;

import java.util.List;
import java.util.Map;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;

import io.delta.kernel.internal.fs.Path;

/**
 * Implements the {@link DataWriteContext} interface. In addition to the data needed for the
 * interface, it also contains the partition values of the targeted partition. In case of
 * un-partitioned tables, the partition values will be empty.
 */
public class DataWriteContextImpl implements DataWriteContext {
    private final String targetDirectory;
    private final Map<String, Literal> partitionValues;
    private final List<Column> statsColumns;

    /**
     * Creates a new instance of WriteContext.
     *
     * @param targetDirectory fully qualified path of the target directory
     * @param partitionValues partition values for the data to be written. If the table is
     *                        un-partitioned, this should be an empty map.
     * @param statsColumns    Set of columns that need statistics for the data to be written.
     *                        The column can be a top-level column or a nested column.
     *                        E.g. "a.b.c" is a nested column. "d" is a top-level column.
     */
    public DataWriteContextImpl(
            String targetDirectory,
            Map<String, Literal> partitionValues,
            List<Column> statsColumns) {
        this.targetDirectory = targetDirectory;
        this.partitionValues = unmodifiableMap(partitionValues);
        this.statsColumns = unmodifiableList(statsColumns);
    }

    /**
     * Returns the target directory where the data should be written.
     *
     * @return fully qualified path of the target directory
     */
    public String getTargetDirectory() {
        // TODO: this is temporary until paths are uniform (i.e. they are actually file system paths
        // or URIs everywhere, but not a combination of the two).
        return new Path(targetDirectory).toUri().toString();
    }

    /**
     * Returns the partition values for the data to be written. If the table is un-partitioned,
     * this should be an empty map.
     *
     * @return partition values
     */
    public Map<String, Literal> getPartitionValues() {
        return partitionValues;
    }

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
    public List<Column> getStatisticsColumns() {
        return statsColumns;
    }
}
