/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Logical removal of a given file from the reservoir. Acts as a tombstone before a file is
 * deleted permanently.
 * <p>
 * Note that users should onlu instantiate {@link RemoveFile} instances using one of the various
 * {@link AddFile#remove()} methods.
 * <p>
 * As well, note that for protocol compatibility reasons, the fields {@code partitionValues},
 * {@code size}, and {@code tags} are only present when the extendedFileMetadata flag is true. New
 * writers should generally be setting this flag, but old writers (and FSCK) won't, so readers must
 * check this flag before attempting to consume those values.
 */
public class RemoveFile implements FileAction {
    @Nonnull
    private final String path;

    @Nonnull
    private final Optional<Long> deletionTimestamp;

    private final boolean dataChange;

    private final boolean extendedFileMetadata;

    @Nullable
    private final Map<String, String> partitionValues;

    private final long size;

    @Nullable
    private final Map<String, String> tags;

    /**
     * Users should <b>not</b> construct {@link RemoveFile}s themselves, and should instead use one
     * of the various {@link AddFile#remove()} methods to instantiate the correct {@link RemoveFile}
     * for a given {@link AddFile} instance.
     */
    public RemoveFile(
            @Nonnull String path,
            @Nonnull Optional<Long> deletionTimestamp,
            boolean dataChange,
            boolean extendedFileMetadata,
            @Nullable Map<String, String> partitionValues,
            long size,
            @Nullable Map<String, String> tags) {
        this.path = path;
        this.deletionTimestamp = deletionTimestamp;
        this.dataChange = dataChange;
        this.extendedFileMetadata = extendedFileMetadata;
        this.partitionValues = partitionValues;
        this.size = size;
        this.tags = tags;
    }

    /**
     * @return the relative path or the absolute path that should be removed from the table. If it's
     *         a relative path, it's relative to the root of the table. Note: the path is encoded
     *         and should be decoded by {@code new java.net.URI(path)} when using it.
     */
    @Override
    public String getPath() {
        return path;
    }

    /**
     * @return the time that this file was deleted as milliseconds since the epoch
     */
    public Optional<Long> getDeletionTimestamp() {
        return deletionTimestamp;
    }

    /**
     * @return whether any data was changed as a result of this file being created. When
     *         {@code false} the file must already be present in the table or the records in the
     *         added file must be contained in one or more remove actions in the same version
     */
    @Override
    public boolean isDataChange() {
        return dataChange;
    }

    /**
     * @return true if the fields {@code partitionValues}, {@code size}, and {@code tags} are
     *         present
     */
    public boolean isExtendedFileMetadata() {
        return extendedFileMetadata;
    }

    /**
     * @return an unmodifiable {@code Map} from partition column to value for
     *         this file. Partition values are stored as strings, using the following formats.
     *         An empty string for any type translates to a null partition value.
     * @see <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Partition-Value-Serialization" target="_blank">Delta Protocol Partition Value Serialization</a>
     */
    @Nullable
    public Map<String, String> getPartitionValues() {
        return partitionValues != null ? Collections.unmodifiableMap(partitionValues) : null;
    }

    /**
     * @return the size of this file in bytes
     */
    public long getSize() {
        return size;
    }

    /**
     * @return an unmodifiable {@code Map} containing metadata about this file
     */
    @Nullable
    public Map<String, String> getTags() {
        return tags != null ? Collections.unmodifiableMap(tags) : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoveFile removeFile = (RemoveFile) o;
        return Objects.equals(path, removeFile.path) &&
                Objects.equals(deletionTimestamp, removeFile.deletionTimestamp) &&
                Objects.equals(dataChange, removeFile.dataChange) &&
                Objects.equals(extendedFileMetadata, removeFile.extendedFileMetadata) &&
                Objects.equals(partitionValues, removeFile.partitionValues) &&
                Objects.equals(size, removeFile.size) &&
                Objects.equals(tags, removeFile.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, deletionTimestamp, dataChange, extendedFileMetadata,
                partitionValues, size, tags);
    }
}
