/*
 * Copyright (2020) The Delta Lake Project Authors.
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
package io.delta.standalone.actions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents an action that adds a new file to the table. The path of a file acts as the primary
 * key for the entry in the set of files.
 *
 * Note: since actions within a given Delta file are not guaranteed to be applied in order, it is
 * not valid for multiple file operations with the same path to exist in a single version.
 *
 * @see  <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md">Delta Transaction Log Protocol</a>
 */
public final class AddFile {
    private final String path;
    private final Map<String, String> partitionValues;
    private final long size;
    private final long modificationTime;
    private final boolean dataChange;
    private final String stats;
    private final Map<String, String> tags;

    public AddFile(String path, Map<String, String> partitionValues, long size,
                   long modificationTime, boolean dataChange, String stats,
                   Map<String, String> tags) {
        this.path = path;
        this.partitionValues = partitionValues;
        this.size = size;
        this.modificationTime = modificationTime;
        this.dataChange = dataChange;
        this.stats = stats;
        this.tags = tags;
    }

    /**
     * @return the relative path or the absolute path that should be added to the table. If it's a
     *         relative path, it's relative to the root of the table. Note: the path is encoded and
     *         should be decoded by {@code new java.net.URI(path)} when using it.
     */
    public String getPath() {
        return path;
    }

    /**
     * @return an unmodifiable {@code Map} from partition column to value for
     *         this file. Partition values are stored as strings, using the following formats.
     *         An empty string for any type translates to a null partition value.
     * @see <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Partition-Value-Serialization" target="_blank">Delta Protocol Partition Value Serialization</a>
     */
    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }

    /**
     * @return the size of this file in bytes
     */
    public long getSize() {
        return size;
    }

    /**
     * @return the time that this file was last modified or created, as
     *         milliseconds since the epoch
     */
    public long getModificationTime() {
        return modificationTime;
    }

    /**
     * @return whether any data was changed as a result of this file being created. When
     *         {@code false} the file must already be present in the table or the records in the
     *         added file must be contained in one or more remove actions in the same version
     */
    public boolean isDataChange() {
        return dataChange;
    }

    /**
     * @return statistics (for example: count, min/max values for columns)
     *         about the data in this file as serialized JSON
     */
    public String getStats() {
        return stats;
    }

    /**
     * @return an unmodifiable {@code Map} containing metadata about this file
     */
    public Map<String, String> getTags() {
        return Collections.unmodifiableMap(tags);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AddFile addFile = (AddFile) o;
        return size == addFile.size &&
                modificationTime == addFile.modificationTime &&
                dataChange == addFile.dataChange &&
                Objects.equals(path, addFile.path) &&
                Objects.equals(partitionValues, addFile.partitionValues) &&
                Objects.equals(stats, addFile.stats) &&
                Objects.equals(tags, addFile.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, partitionValues, size, modificationTime, dataChange, stats, tags);
    }
}
