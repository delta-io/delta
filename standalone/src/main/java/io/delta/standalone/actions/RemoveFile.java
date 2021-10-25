// TODO: copyright

package io.delta.standalone.actions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical removal of a given file from the reservoir. Acts as a tombstone before a file is
 * deleted permanently.
 *
 * Note that for protocol compatibility reasons, the fields {@code partitionValues}, {@code size},
 * and {@code tags} are only present when the extendedFileMetadata flag is true. New writers should
 * generally be setting this flag, but old writers (and FSCK) won't, so readers must check this flag
 * before attempting to consume those values.
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

    public RemoveFile(@Nonnull String path, @Nonnull Optional<Long> deletionTimestamp,
                      boolean dataChange, boolean extendedFileMetadata,
                      @Nullable Map<String, String> partitionValues, long size,
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

    /**
     * @return a new {@code RemoveFile.Builder}
     */
    public static Builder builder(String path) {
        return new Builder(path);
    }

    /**
     * Builder class for RemoveFile. Enables construction of RemoveFile object with default values.
     */
    public static class Builder {
        // required RemoveFile fields
        private final String path;
        // optional RemoveFile fields
        private Optional<Long> deletionTimestamp = Optional.empty();
        private boolean dataChange = true;
        private boolean extendedFileMetadata = false;
        private Map<String, String> partitionValues;
        private long size = 0;
        private Map<String, String> tags;

        public Builder(String path) {
            this.path = path;
        }

        public Builder deletionTimestamp(Long deletionTimestamp) {
            this.deletionTimestamp = Optional.of(deletionTimestamp);
            return this;
        }

        public Builder dataChange(boolean dataChange) {
            this.dataChange = dataChange;
            return this;
        }

        public Builder extendedFileMetadata(boolean extendedFileMetadata) {
            this.extendedFileMetadata = extendedFileMetadata;
            return this;
        }

        public Builder partitionValues(Map<String, String> partitionValues) {
            this.partitionValues = partitionValues;
            return this;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder tags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        /**
         * @return a new {@code RemoveFile} with the same properties as {@code this}
         */
        public RemoveFile build() {
            RemoveFile removeFile = new RemoveFile(
                    this.path,
                    this.deletionTimestamp,
                    this.dataChange,
                    this.extendedFileMetadata,
                    this.partitionValues,
                    this.size,
                    this.tags);
            return removeFile;
        }
    }
}
