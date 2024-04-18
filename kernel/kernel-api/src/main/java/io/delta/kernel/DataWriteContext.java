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
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;

/**
 * Contains the context for writing data related to a partition to Delta table.
 * @since 3.2.0
 */
@Evolving
public class DataWriteContext {
    private final String targetDirectory;
    private final long targetFileSizeInBytes;
    private final Map<String, Literal> partitionValues;
    private final List<Column> statsColumns;

    /**
     * Creates a new instance of WriteContext.
     *
     * @param partitionPath         fully qualified path of the target directory
     * @param targetFileSizeInBytes target file size in bytes
     * @param partitionValues       partition values
     * @param statsColumns          schema of the statistics
     */
    public DataWriteContext(
            String partitionPath,
            long targetFileSizeInBytes,
            Map<String, Literal> partitionValues,
            List<Column> statsColumns) {
        this.targetDirectory = partitionPath;
        this.targetFileSizeInBytes = targetFileSizeInBytes;
        this.partitionValues = partitionValues;
        this.statsColumns = statsColumns;
    }

    /**
     * Returns the target directory where the data should be written.
     *
     * @return fully qualified path of the target directory
     */
    public String getTargetDirectory() {
        return targetDirectory;
    }

    /**
     * Returns the target file size in bytes for the data to be written. Connector can aim to write
     * data in files of this size, but not mandatory
     *
     * @return target file size in bytes
     */
    public long getTargetFileSizeInBytes() {
        return targetFileSizeInBytes;
    }

    /**
     * Returns the partition values for the data to be written.
     *
     * @return partition values
     */
    public Map<String, Literal> getPartitionValues() {
        return partitionValues;
    }

    /**
     * Returns the list of columns that need to statistics for the data to be written. Statistics
     * collections is optional, but when present can be used to optimize query performance.
     *
     * @return schema of the statistics
     */
    public List<Column> getStatisticsColumns() {
        return statsColumns;
    }
}
