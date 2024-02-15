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

package io.delta.kernel.utils;

import java.util.Optional;

/**
 * Extends {@link FileStatus} to include additional details such as column level statistics
 * of the data file in the Delta Lake table.
 */
public class DataFileStatus extends FileStatus {

    private final Optional<DataFileStatistics> statistics;

    /**
     * Create a new instance of {@link DataFileStatus}.
     *
     * @param path Fully qualified file path.
     * @param size File size in bytes.
     * @param modificationTime Last modification time of the file in epoch milliseconds.
     * @param statistics Optional column and file level statistics in the data file.
     */
    public DataFileStatus(
            String path,
            long size,
            long modificationTime,
            Optional<DataFileStatistics> statistics) {
        super(path, size, modificationTime);
        this.statistics = statistics;
    }

    /**
     * Get the statistics of the data file encapsulated in this object.
     *
     * @return Statistics of the file.
     */
    public Optional<DataFileStatistics> getStatistics() {
        return statistics;
    }
}
